/*-------------------------------------------------------------------------
 *
 * wait.c
 *	  Implements WAIT FOR, which allows waiting for events such as
 *	  time passing or LSN having been replayed, flushed, or written.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/commands/wait.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogwait.h"
#include "commands/defrem.h"
#include "commands/wait.h"
#include "executor/executor.h"
#include "parser/parse_node.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"


/*
 * Type descriptor for WAIT FOR LSN wait types, used for error messages.
 */
typedef struct WaitLSNTypeDesc
{
	const char *noun;			/* "replay", "flush", "write" */
	const char *verb;			/* "replayed", "flushed", "written" */
}			WaitLSNTypeDesc;

static const WaitLSNTypeDesc WaitLSNTypeDescs[] = {
	[WAIT_LSN_TYPE_STANDBY_REPLAY] = {"replay", "replayed"},
	[WAIT_LSN_TYPE_STANDBY_WRITE] = {"write", "written"},
	[WAIT_LSN_TYPE_STANDBY_FLUSH] = {"flush", "flushed"},
};

void
ExecWaitStmt(ParseState *pstate, WaitStmt *stmt, DestReceiver *dest)
{
	XLogRecPtr	lsn;
	int64		timeout = 0;
	WaitLSNResult waitLSNResult;
	WaitLSNType lsnType;
	bool		throw = true;
	TupleDesc	tupdesc;
	TupOutputState *tstate;
	const char *result = "<unset>";
	bool		timeout_specified = false;
	bool		no_throw_specified = false;

	/* Parse and validate the mandatory LSN */
	lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in,
										  CStringGetDatum(stmt->lsn_literal)));

	/* Convert parse-time WaitLSNMode to runtime WaitLSNType */
	switch (stmt->mode)
	{
		case WAIT_LSN_MODE_REPLAY:
			lsnType = WAIT_LSN_TYPE_STANDBY_REPLAY;
			break;
		case WAIT_LSN_MODE_WRITE:
			lsnType = WAIT_LSN_TYPE_STANDBY_WRITE;
			break;
		case WAIT_LSN_MODE_FLUSH:
			lsnType = WAIT_LSN_TYPE_STANDBY_FLUSH;
			break;
		default:
			elog(ERROR, "unrecognized wait mode: %d", stmt->mode);
	}

	foreach_node(DefElem, defel, stmt->options)
	{
		if (strcmp(defel->defname, "timeout") == 0)
		{
			char	   *timeout_str;
			const char *hintmsg;
			double		result;

			if (timeout_specified)
				errorConflictingDefElem(defel, pstate);
			timeout_specified = true;

			timeout_str = defGetString(defel);

			if (!parse_real(timeout_str, &result, GUC_UNIT_MS, &hintmsg))
			{
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid timeout value: \"%s\"", timeout_str),
						hintmsg ? errhint("%s", _(hintmsg)) : 0);
			}

			/*
			 * Get rid of any fractional part in the input. This is so we
			 * don't fail on just-out-of-range values that would round into
			 * range.
			 */
			result = rint(result);

			/* Range check */
			if (unlikely(isnan(result) || !FLOAT8_FITS_IN_INT64(result)))
				ereport(ERROR,
						errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("timeout value is out of range"));

			if (result < 0)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("timeout cannot be negative"));

			timeout = (int64) result;
		}
		else if (strcmp(defel->defname, "no_throw") == 0)
		{
			if (no_throw_specified)
				errorConflictingDefElem(defel, pstate);

			no_throw_specified = true;

			throw = !defGetBoolean(defel);
		}
		else
		{
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("option \"%s\" not recognized",
						   defel->defname),
					parser_errposition(pstate, defel->location));
		}
	}

	/*
	 * We are going to wait for the LSN.  We should first care that we don't
	 * hold a snapshot and correspondingly our MyProc->xmin is invalid.
	 * Otherwise, our snapshot could prevent the replay of WAL records
	 * implying a kind of self-deadlock.  This is the reason why WAIT FOR is a
	 * command, not a procedure or function.
	 *
	 * At first, we should check there is no active snapshot.  According to
	 * PlannedStmtRequiresSnapshot(), even in an atomic context, CallStmt is
	 * processed with a snapshot.  Thankfully, we can pop this snapshot,
	 * because PortalRunUtility() can tolerate this.
	 */
	if (ActiveSnapshotSet())
		PopActiveSnapshot();

	/*
	 * At second, invalidate a catalog snapshot if any.  And we should be done
	 * with the preparation.
	 */
	InvalidateCatalogSnapshot();

	/* Give up if there is still an active or registered snapshot. */
	if (HaveRegisteredOrActiveSnapshot())
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("WAIT FOR must be only called without an active or registered snapshot"),
				errdetail("WAIT FOR cannot be executed from a function or a procedure or within a transaction with an isolation level higher than READ COMMITTED."));

	/*
	 * As the result we should hold no snapshot, and correspondingly our xmin
	 * should be unset.
	 */
	Assert(MyProc->xmin == InvalidTransactionId);

	waitLSNResult = WaitForLSN(lsnType, lsn, timeout);

	/*
	 * Process the result of WaitForLSN().  Throw appropriate error if needed.
	 */
	switch (waitLSNResult)
	{
		case WAIT_LSN_RESULT_SUCCESS:
			/* Nothing to do on success */
			result = "success";
			break;

		case WAIT_LSN_RESULT_TIMEOUT:
			if (throw)
			{
				const		WaitLSNTypeDesc *desc = &WaitLSNTypeDescs[lsnType];
				XLogRecPtr	currentLSN = GetCurrentLSNForWaitType(lsnType);

				ereport(ERROR,
						errcode(ERRCODE_QUERY_CANCELED),
						errmsg("timed out while waiting for target LSN %X/%08X to be %s; current %s LSN %X/%08X",
							   LSN_FORMAT_ARGS(lsn),
							   desc->verb,
							   desc->noun,
							   LSN_FORMAT_ARGS(currentLSN)));
			}
			else
				result = "timeout";
			break;

		case WAIT_LSN_RESULT_NOT_IN_RECOVERY:
			if (throw)
			{
				const		WaitLSNTypeDesc *desc = &WaitLSNTypeDescs[lsnType];
				XLogRecPtr	currentLSN = GetCurrentLSNForWaitType(lsnType);

				if (PromoteIsTriggered())
				{
					ereport(ERROR,
							errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("recovery is not in progress"),
							errdetail("Recovery ended before target LSN %X/%08X was %s; last %s LSN %X/%08X.",
									  LSN_FORMAT_ARGS(lsn),
									  desc->verb,
									  desc->noun,
									  LSN_FORMAT_ARGS(currentLSN)));
				}
				else
					ereport(ERROR,
							errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("recovery is not in progress"),
							errhint("Waiting for the %s LSN can only be executed during recovery.",
									desc->noun));
			}
			else
				result = "not in recovery";
			break;
	}

	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = WaitStmtResultDesc(stmt);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

	/* Send it */
	do_text_output_oneline(tstate, result);

	end_tup_output(tstate);
}

TupleDesc
WaitStmtResultDesc(WaitStmt *stmt)
{
	TupleDesc	tupdesc;

	/* Need a tuple descriptor representing a single TEXT  column */
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "status",
					   TEXTOID, -1, 0);
	return tupdesc;
}
