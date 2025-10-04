/*-------------------------------------------------------------------------
 *
 * xlogwait.h
 *	  Declarations for LSN replay waiting routines.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * src/include/access/xlogwait.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XLOG_WAIT_H
#define XLOG_WAIT_H

#include "lib/pairingheap.h"
#include "port/atomics.h"
#include "postgres.h"
#include "storage/procnumber.h"
#include "storage/spin.h"
#include "tcop/dest.h"

/*
 * Result statuses for WaitForLSNReplay().
 */
typedef enum
{
	WAIT_LSN_RESULT_SUCCESS,	/* Target LSN is reached */
	WAIT_LSN_RESULT_TIMEOUT,	/* Timeout occurred */
	WAIT_LSN_RESULT_NOT_IN_RECOVERY,	/* Recovery ended before or during our
										 * wait */
} WaitLSNResult;

/*
 * Wait operation types for LSN waiting facility.
 */
typedef enum WaitLSNOperation
{
	WAIT_LSN_REPLAY,	/* Waiting for replay on standby */
	WAIT_LSN_FLUSH		/* Waiting for flush on primary */
} WaitLSNOperation;

/*
 * WaitLSNProcInfo - the shared memory structure representing information
 * about the single process, which may wait for LSN operations.  An item of
 * waitLSNState->procInfos array.
 */
typedef struct WaitLSNProcInfo
{
	/* LSN, which this process is waiting for */
	XLogRecPtr	waitLSN;

	/* Process to wake up once the waitLSN is reached */
	ProcNumber	procno;

	/* Type-safe heap membership flags */
	bool		inReplayHeap;	/* In replay waiters heap */
	bool		inFlushHeap;	/* In flush waiters heap */

	/* Separate heap nodes for type safety */
	pairingheap_node replayHeapNode;
	pairingheap_node flushHeapNode;
} WaitLSNProcInfo;

/*
 * WaitLSNState - the shared memory state for the LSN waiting facility.
 */
typedef struct WaitLSNState
{
	/*
	 * The minimum replay LSN value some process is waiting for.  Used for the
	 * fast-path checking if we need to wake up any waiters after replaying a
	 * WAL record.  Could be read lock-less.  Update protected by WaitLSNLock.
	 */
	pg_atomic_uint64 minWaitedReplayLSN;

	/*
	 * A pairing heap of replay waiting processes ordered by LSN values (least LSN is
	 * on top).  Protected by WaitLSNLock.
	 */
	pairingheap replayWaitersHeap;

	/*
	 * The minimum flush LSN value some process is waiting for.  Used for the
	 * fast-path checking if we need to wake up any waiters after flushing
	 * WAL.  Could be read lock-less.  Update protected by WaitLSNLock.
	 */
	pg_atomic_uint64 minWaitedFlushLSN;

	/*
	 * A pairing heap of flush waiting processes ordered by LSN values (least LSN is
	 * on top).  Protected by WaitLSNLock.
	 */
	pairingheap flushWaitersHeap;

	/*
	 * An array with per-process information, indexed by the process number.
	 * Protected by WaitLSNLock.
	 */
	WaitLSNProcInfo procInfos[FLEXIBLE_ARRAY_MEMBER];
} WaitLSNState;


extern PGDLLIMPORT WaitLSNState *waitLSNState;

extern Size WaitLSNShmemSize(void);
extern void WaitLSNShmemInit(void);
extern void WaitLSNWakeupReplay(XLogRecPtr currentLSN);
extern void WaitLSNWakeupFlush(XLogRecPtr currentLSN);
extern void WaitLSNCleanup(void);
extern WaitLSNResult WaitForLSNReplay(XLogRecPtr targetLSN, int64 timeout);
extern void WaitForLSNFlush(XLogRecPtr targetLSN);

#endif							/* XLOG_WAIT_H */
