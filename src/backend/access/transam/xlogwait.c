/*-------------------------------------------------------------------------
 *
 * xlogwait.c
 *	  Implements waiting for the given replay LSN, which is used in
 *	  WAIT FOR lsn '...'
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/xlogwait.c
 *
 * NOTES
 *		This file implements waiting for the replay of the given LSN on a
 *		physical standby.  The core idea is very small: every backend that
 *		wants to wait publishes the LSN it needs to the shared memory, and
 *		the startup process wakes it once that LSN has been replayed.
 *
 *		The shared memory used by this module comprises a procInfos
 *		per-backend array with the information of the awaited LSN for each
 *		of the backend processes.  The elements of that array are organized
 *		into a pairing heap waitersHeap, which allows for very fast finding
 *		of the least awaited LSN.
 *
 *		In addition, the least-awaited LSN is cached as minWaitedLSN.  The
 *		waiter process publishes information about itself to the shared
 *		memory and waits on the latch before it wakens up by a startup
 *		process, timeout is reached, standby is promoted, or the postmaster
 *		dies.  Then, it cleans information about itself in the shared memory.
 *
 *		After replaying a WAL record, the startup process first performs a
 *		fast path check minWaitedLSN > replayLSN.  If this check is negative,
 *		it checks waitersHeap and wakes up the backend whose awaited LSNs
 *		are reached.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "access/xlogwait.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/fmgrprotos.h"
#include "utils/pg_lsn.h"
#include "utils/snapmgr.h"



static int	waitlsn_cmp(const pairingheap_node *a, const pairingheap_node *b,
						void *arg);

struct WaitLSNState *waitLSNState = NULL;

/* Report the amount of shared memory space needed for WaitLSNState. */
Size
WaitLSNShmemSize(void)
{
	Size		size;

	size = offsetof(WaitLSNState, procInfos);
	size = add_size(size, mul_size(MaxBackends + NUM_AUXILIARY_PROCS, sizeof(WaitLSNProcInfo)));
	return size;
}

/* Initialize the WaitLSNState in the shared memory. */
void
WaitLSNShmemInit(void)
{
	bool		found;

	waitLSNState = (WaitLSNState *) ShmemInitStruct("WaitLSNState",
														  WaitLSNShmemSize(),
														  &found);
	if (!found)
	{
		pg_atomic_init_u64(&waitLSNState->minWaitedLSN, PG_UINT64_MAX);
		pairingheap_initialize(&waitLSNState->waitersHeap, waitlsn_cmp, NULL);
		memset(&waitLSNState->procInfos, 0,
			   (MaxBackends + NUM_AUXILIARY_PROCS) * sizeof(WaitLSNProcInfo));
	}
}

/*
 * Comparison function for waitReplayLSN->waitersHeap heap.  Waiting processes are
 * ordered by lsn, so that the waiter with smallest lsn is at the top.
 */
static int
waitlsn_cmp(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	const WaitLSNProcInfo *aproc = pairingheap_const_container(WaitLSNProcInfo, phNode, a);
	const WaitLSNProcInfo *bproc = pairingheap_const_container(WaitLSNProcInfo, phNode, b);

	if (aproc->waitLSN < bproc->waitLSN)
		return 1;
	else if (aproc->waitLSN > bproc->waitLSN)
		return -1;
	else
		return 0;
}

/*
 * Update waitReplayLSN->minWaitedLSN according to the current state of
 * waitReplayLSN->waitersHeap.
 */
static void
updateMinWaitedLSN(void)
{
	XLogRecPtr	minWaitedLSN = PG_UINT64_MAX;

	if (!pairingheap_is_empty(&waitLSNState->waitersHeap))
	{
		pairingheap_node *node = pairingheap_first(&waitLSNState->waitersHeap);

		minWaitedLSN = pairingheap_container(WaitLSNProcInfo, phNode, node)->waitLSN;
	}

	pg_atomic_write_u64(&waitLSNState->minWaitedLSN, minWaitedLSN);
}

/*
 * Put the current process into the heap of LSN waiters.
 */
static void
addLSNWaiter(XLogRecPtr lsn)
{
	WaitLSNProcInfo *procInfo = &waitLSNState->procInfos[MyProcNumber];

	LWLockAcquire(WaitLSNLock, LW_EXCLUSIVE);

	Assert(!procInfo->inHeap);

	procInfo->procno = MyProcNumber;
	procInfo->waitLSN = lsn;

	pairingheap_add(&waitLSNState->waitersHeap, &procInfo->phNode);
	procInfo->inHeap = true;
	updateMinWaitedLSN();

	LWLockRelease(WaitLSNLock);
}

/*
 * Remove the current process from the heap of LSN waiters if it's there.
 */
static void
deleteLSNWaiter(void)
{
	WaitLSNProcInfo *procInfo = &waitLSNState->procInfos[MyProcNumber];

	LWLockAcquire(WaitLSNLock, LW_EXCLUSIVE);

	if (!procInfo->inHeap)
	{
		LWLockRelease(WaitLSNLock);
		return;
	}

	pairingheap_remove(&waitLSNState->waitersHeap, &procInfo->phNode);
	procInfo->inHeap = false;
	updateMinWaitedLSN();

	LWLockRelease(WaitLSNLock);
}

/*
 * Size of a static array of procs to wakeup by WaitLSNWakeup() allocated
 * on the stack.  It should be enough to take single iteration for most cases.
 */
#define	WAKEUP_PROC_STATIC_ARRAY_SIZE (16)

/*
 * Remove waiters whose LSN has been replayed from the heap and set their
 * latches.  If InvalidXLogRecPtr is given, remove all waiters from the heap
 * and set latches for all waiters.
 *
 * This function first accumulates waiters to wake up into an array, then
 * wakes them up without holding a WaitLSNLock.  The array size is static and
 * equal to WAKEUP_PROC_STATIC_ARRAY_SIZE.  That should be more than enough
 * to wake up all the waiters at once in the vast majority of cases.  However,
 * if there are more waiters, this function will loop to process them in
 * multiple chunks.
 */
void
WaitLSNWakeup(XLogRecPtr currentLSN)
{
	int			i;
	ProcNumber	wakeUpProcs[WAKEUP_PROC_STATIC_ARRAY_SIZE];
	int			numWakeUpProcs;

	do
	{
		numWakeUpProcs = 0;
		LWLockAcquire(WaitLSNLock, LW_EXCLUSIVE);

		/*
		 * Iterate the pairing heap of waiting processes till we find LSN not
		 * yet replayed.  Record the process numbers to wake up, but to avoid
		 * holding the lock for too long, send the wakeups only after
		 * releasing the lock.
		 */
		while (!pairingheap_is_empty(&waitLSNState->waitersHeap))
		{
			pairingheap_node *node = pairingheap_first(&waitLSNState->waitersHeap);
			WaitLSNProcInfo *procInfo = pairingheap_container(WaitLSNProcInfo, phNode, node);

			if (!XLogRecPtrIsInvalid(currentLSN) &&
				procInfo->waitLSN > currentLSN)
				break;

			Assert(numWakeUpProcs < WAKEUP_PROC_STATIC_ARRAY_SIZE);
			wakeUpProcs[numWakeUpProcs++] = procInfo->procno;
			(void) pairingheap_remove_first(&waitLSNState->waitersHeap);
			procInfo->inHeap = false;

			if (numWakeUpProcs == WAKEUP_PROC_STATIC_ARRAY_SIZE)
				break;
		}

		updateMinWaitedLSN();

		LWLockRelease(WaitLSNLock);

		/*
		 * Set latches for processes, whose waited LSNs are already replayed.
		 * As the time consuming operations, we do this outside of
		 * WaitLSNLock. This is  actually fine because procLatch isn't ever
		 * freed, so we just can potentially set the wrong process' (or no
		 * process') latch.
		 */
		for (i = 0; i < numWakeUpProcs; i++)
			SetLatch(&GetPGProcByNumber(wakeUpProcs[i])->procLatch);

		/* Need to recheck if there were more waiters than static array size. */
	}
	while (numWakeUpProcs == WAKEUP_PROC_STATIC_ARRAY_SIZE);
}

/*
 * Delete our item from shmem array if any.
 */
void
WaitLSNCleanup(void)
{
	/*
	 * We do a fast-path check of the 'inHeap' flag without the lock.  This
	 * flag is set to true only by the process itself.  So, it's only possible
	 * to get a false positive.  But that will be eliminated by a recheck
	 * inside deleteLSNWaiter().
	 */
	if (waitLSNState->procInfos[MyProcNumber].inHeap)
		deleteLSNWaiter();
}

/*
 * Wait using MyLatch till the given LSN is replayed, a timeout happens, the
 * replica gets promoted, or the postmaster dies.
 *
 * Returns WAIT_LSN_RESULT_SUCCESS if target LSN was replayed.  Returns
 * WAIT_LSN_RESULT_TIMEOUT if the timeout was reached before the target LSN
 * replayed.  Returns WAIT_LSN_RESULT_NOT_IN_RECOVERY if run not in recovery,
 * or replica got promoted before the target LSN replayed.
 */
WaitLSNResult
WaitForLSNReplay(XLogRecPtr targetLSN, int64 timeout)
{
	XLogRecPtr	currentLSN;
	TimestampTz endtime = 0;
	int			wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH;

	/* Shouldn't be called when shmem isn't initialized */
	Assert(waitLSNState);

	/* Should have a valid proc number */
	Assert(MyProcNumber >= 0 && MyProcNumber < MaxBackends);

	if (!RecoveryInProgress())
	{
		/*
		 * Recovery is not in progress.  Given that we detected this in the
		 * very first check, this procedure was mistakenly called on primary.
		 * However, it's possible that standby was promoted concurrently to
		 * the procedure call, while target LSN is replayed.  So, we still
		 * check the last replay LSN before reporting an error.
		 */
		if (PromoteIsTriggered() && targetLSN <= GetXLogReplayRecPtr(NULL))
			return WAIT_LSN_RESULT_SUCCESS;
		return WAIT_LSN_RESULT_NOT_IN_RECOVERY;
	}
	else
	{
		/* If target LSN is already replayed, exit immediately */
		if (targetLSN <= GetXLogReplayRecPtr(NULL))
			return WAIT_LSN_RESULT_SUCCESS;
	}

	if (timeout > 0)
	{
		endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout);
		wake_events |= WL_TIMEOUT;
	}

	/*
	 * Add our process to the pairing heap of waiters.  It might happen that
	 * target LSN gets replayed before we do.  Another check at the beginning
	 * of the loop below prevents the race condition.
	 */
	addLSNWaiter(targetLSN);

	for (;;)
	{
		int			rc;
		long		delay_ms = 0;

		/* Recheck that recovery is still in-progress */
		if (!RecoveryInProgress())
		{
			/*
			 * Recovery was ended, but recheck if target LSN was already
			 * replayed.  See the comment regarding deleteLSNWaiter() below.
			 */
			deleteLSNWaiter();
			currentLSN = GetXLogReplayRecPtr(NULL);
			if (PromoteIsTriggered() && targetLSN <= currentLSN)
				return WAIT_LSN_RESULT_SUCCESS;
			return WAIT_LSN_RESULT_NOT_IN_RECOVERY;
		}
		else
		{
			/* Check if the waited LSN has been replayed */
			currentLSN = GetXLogReplayRecPtr(NULL);
			if (targetLSN <= currentLSN)
				break;
		}

		/*
		 * If the timeout value is specified, calculate the number of
		 * milliseconds before the timeout.  Exit if the timeout is already
		 * reached.
		 */
		if (timeout > 0)
		{
			delay_ms = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), endtime);
			if (delay_ms <= 0)
				break;
		}

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch, wake_events, delay_ms,
					   WAIT_EVENT_WAIT_FOR_WAL_REPLAY);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection due to unexpected postmaster exit"),
					 errcontext("while waiting for LSN replay")));

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	/*
	 * Delete our process from the shared memory pairing heap.  We might
	 * already be deleted by the startup process.  The 'inHeap' flag prevents
	 * us from the double deletion.
	 */
	deleteLSNWaiter();

	/*
	 * If we didn't reach the target LSN, we must be exited by timeout.
	 */
	if (targetLSN > currentLSN)
		return WAIT_LSN_RESULT_TIMEOUT;

	return WAIT_LSN_RESULT_SUCCESS;
}
