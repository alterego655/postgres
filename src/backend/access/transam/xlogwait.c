/*-------------------------------------------------------------------------
 *
 * xlogwait.c
 *	  Implements waiting for WAL operations to reach specific LSNs.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/xlogwait.c
 *
 * NOTES
 *		This file implements waiting for WAL operations to reach specific LSNs
 *		on both physical standby and primary servers. The core idea is simple:
 *		every process that wants to wait publishes the LSN it needs to the
 *		shared memory, and the appropriate process (startup on standby, or
 *		WAL writer/backend on primary) wakes it once that LSN has been reached.
 *
 *		The shared memory used by this module comprises a procInfos
 *		per-backend array with the information of the awaited LSN for each
 *		of the backend processes.  The elements of that array are organized
 *		into a pairing heap waitersHeap, which allows for very fast finding
 *		of the least awaited LSN.
 *
 *		In addition, the least-awaited LSN is cached as minWaitedLSN.  The
 *		waiter process publishes information about itself to the shared
 *		memory and waits on the latch before it wakens up by the appropriate
 *		process, standby is promoted, or the postmaster	dies.  Then, it cleans
 *		information about itself in the shared memory.
 *
 *		On standby servers: After replaying a WAL record, the startup process
 *		first performs a fast path check minWaitedLSN > replayLSN.  If this
 *		check is negative, it checks waitersHeap and wakes up the backend
 *		whose awaited LSNs are reached.
 *
 *		On primary servers: After flushing WAL, the WAL writer or backend
 *		process performs a similar check against the flush LSN and wakes up
 *		waiters whose target flush LSNs have been reached.
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


static int	waitlsn_replay_cmp(const pairingheap_node *a, const pairingheap_node *b,
						void *arg);

static int	waitlsn_flush_cmp(const pairingheap_node *a, const pairingheap_node *b,
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
		/* Initialize replay heap and tracking */
		pg_atomic_init_u64(&waitLSNState->minWaitedReplayLSN, PG_UINT64_MAX);
		pairingheap_initialize(&waitLSNState->replayWaitersHeap, waitlsn_replay_cmp, (void *)(uintptr_t)WAIT_LSN_REPLAY);

		/* Initialize flush heap and tracking */
		pg_atomic_init_u64(&waitLSNState->minWaitedFlushLSN, PG_UINT64_MAX);
		pairingheap_initialize(&waitLSNState->flushWaitersHeap, waitlsn_flush_cmp, (void *)(uintptr_t)WAIT_LSN_FLUSH);

		/* Initialize process info array */
		memset(&waitLSNState->procInfos, 0,
			   (MaxBackends + NUM_AUXILIARY_PROCS) * sizeof(WaitLSNProcInfo));
	}
}

/*
 * Comparison function for replay waiters heaps. Waiting processes are
 * ordered by LSN, so that the waiter with smallest LSN is at the top.
 */
static int
waitlsn_replay_cmp(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	const WaitLSNProcInfo *aproc = pairingheap_const_container(WaitLSNProcInfo, replayHeapNode, a);
	const WaitLSNProcInfo *bproc = pairingheap_const_container(WaitLSNProcInfo, replayHeapNode, b);

	if (aproc->waitLSN < bproc->waitLSN)
		return 1;
	else if (aproc->waitLSN > bproc->waitLSN)
		return -1;
	else
		return 0;
}

/*
 * Comparison function for flush waiters heaps. Waiting processes are
 * ordered by LSN, so that the waiter with smallest LSN is at the top.
 */
static int
waitlsn_flush_cmp(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	const WaitLSNProcInfo *aproc = pairingheap_const_container(WaitLSNProcInfo, flushHeapNode, a);
	const WaitLSNProcInfo *bproc = pairingheap_const_container(WaitLSNProcInfo, flushHeapNode, b);

	if (aproc->waitLSN < bproc->waitLSN)
		return 1;
	else if (aproc->waitLSN > bproc->waitLSN)
		return -1;
	else
		return 0;
}

/*
 * Update minimum waited LSN for the specified operation type
 */
static void
updateMinWaitedLSN(WaitLSNOperation operation)
{
	XLogRecPtr minWaitedLSN = PG_UINT64_MAX;

	if (operation == WAIT_LSN_REPLAY)
	{
		if (!pairingheap_is_empty(&waitLSNState->replayWaitersHeap))
		{
			pairingheap_node *node = pairingheap_first(&waitLSNState->replayWaitersHeap);
			WaitLSNProcInfo *procInfo = pairingheap_container(WaitLSNProcInfo, replayHeapNode, node);
			minWaitedLSN = procInfo->waitLSN;
		}
		pg_atomic_write_u64(&waitLSNState->minWaitedReplayLSN, minWaitedLSN);
	}
	else /* WAIT_LSN_FLUSH */
	{
		if (!pairingheap_is_empty(&waitLSNState->flushWaitersHeap))
		{
			pairingheap_node *node = pairingheap_first(&waitLSNState->flushWaitersHeap);
			WaitLSNProcInfo *procInfo = pairingheap_container(WaitLSNProcInfo, flushHeapNode, node);
			minWaitedLSN = procInfo->waitLSN;
		}
		pg_atomic_write_u64(&waitLSNState->minWaitedFlushLSN, minWaitedLSN);
	}
}

/*
 * Add current process to appropriate waiters heap based on operation type
 */
static void
addLSNWaiter(XLogRecPtr lsn, WaitLSNOperation operation)
{
	WaitLSNProcInfo *procInfo = &waitLSNState->procInfos[MyProcNumber];

	LWLockAcquire(WaitLSNLock, LW_EXCLUSIVE);

	procInfo->procno = MyProcNumber;
	procInfo->waitLSN = lsn;

	if (operation == WAIT_LSN_REPLAY)
	{
		Assert(!procInfo->inReplayHeap);
		pairingheap_add(&waitLSNState->replayWaitersHeap, &procInfo->replayHeapNode);
		procInfo->inReplayHeap = true;
		updateMinWaitedLSN(WAIT_LSN_REPLAY);
	}
	else /* WAIT_LSN_FLUSH */
	{
		Assert(!procInfo->inFlushHeap);
		pairingheap_add(&waitLSNState->flushWaitersHeap, &procInfo->flushHeapNode);
		procInfo->inFlushHeap = true;
		updateMinWaitedLSN(WAIT_LSN_FLUSH);
	}

	LWLockRelease(WaitLSNLock);
}

/*
 * Remove current process from appropriate waiters heap based on operation type
 */
static void
deleteLSNWaiter(WaitLSNOperation operation)
{
	WaitLSNProcInfo *procInfo = &waitLSNState->procInfos[MyProcNumber];

	LWLockAcquire(WaitLSNLock, LW_EXCLUSIVE);

	if (operation == WAIT_LSN_REPLAY && procInfo->inReplayHeap)
	{
		pairingheap_remove(&waitLSNState->replayWaitersHeap, &procInfo->replayHeapNode);
		procInfo->inReplayHeap = false;
		updateMinWaitedLSN(WAIT_LSN_REPLAY);
	}
	else if (operation == WAIT_LSN_FLUSH && procInfo->inFlushHeap)
	{
		pairingheap_remove(&waitLSNState->flushWaitersHeap, &procInfo->flushHeapNode);
		procInfo->inFlushHeap = false;
		updateMinWaitedLSN(WAIT_LSN_FLUSH);
	}

	LWLockRelease(WaitLSNLock);
}

/*
 * Size of a static array of procs to wakeup by WaitLSNWakeup() allocated
 * on the stack.  It should be enough to take single iteration for most cases.
 */
#define	WAKEUP_PROC_STATIC_ARRAY_SIZE (16)

/*
 * Remove waiters whose LSN has been reached from the heap and set their
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
static void
wakeupWaiters(WaitLSNOperation operation, XLogRecPtr currentLSN)
{
	ProcNumber wakeUpProcs[WAKEUP_PROC_STATIC_ARRAY_SIZE];
	int numWakeUpProcs;
	int i;
	pairingheap *heap;

	/* Select appropriate heap */
	heap = (operation == WAIT_LSN_REPLAY) ?
		   &waitLSNState->replayWaitersHeap :
		   &waitLSNState->flushWaitersHeap;

	do
	{
		numWakeUpProcs = 0;
		LWLockAcquire(WaitLSNLock, LW_EXCLUSIVE);

		/*
		 * Iterate the waiters heap until we find LSN not yet reached.
		 * Record process numbers to wake up, but send wakeups after releasing lock.
		 */
		while (!pairingheap_is_empty(heap))
		{
			pairingheap_node *node = pairingheap_first(heap);
			WaitLSNProcInfo *procInfo;

			/* Get procInfo using appropriate heap node */
			if (operation == WAIT_LSN_REPLAY)
				procInfo = pairingheap_container(WaitLSNProcInfo, replayHeapNode, node);
			else
				procInfo = pairingheap_container(WaitLSNProcInfo, flushHeapNode, node);

			if (!XLogRecPtrIsInvalid(currentLSN) && procInfo->waitLSN > currentLSN)
				break;

			Assert(numWakeUpProcs < WAKEUP_PROC_STATIC_ARRAY_SIZE);
			wakeUpProcs[numWakeUpProcs++] = procInfo->procno;
			(void) pairingheap_remove_first(heap);

			/* Update appropriate flag */
			if (operation == WAIT_LSN_REPLAY)
				procInfo->inReplayHeap = false;
			else
				procInfo->inFlushHeap = false;

			if (numWakeUpProcs == WAKEUP_PROC_STATIC_ARRAY_SIZE)
				break;
		}

		updateMinWaitedLSN(operation);
		LWLockRelease(WaitLSNLock);

		/*
		 * Set latches for processes, whose waited LSNs are already reached.
		 * As the time consuming operations, we do this outside of
		 * WaitLSNLock. This is  actually fine because procLatch isn't ever
		 * freed, so we just can potentially set the wrong process' (or no
		 * process') latch.
		 */
		for (i = 0; i < numWakeUpProcs; i++)
			SetLatch(&GetPGProcByNumber(wakeUpProcs[i])->procLatch);

	} while (numWakeUpProcs == WAKEUP_PROC_STATIC_ARRAY_SIZE);
}

/*
 * Wake up processes waiting for replay LSN to reach currentLSN
 */
void
WaitLSNWakeupReplay(XLogRecPtr currentLSN)
{
	/* Fast path check */
	if (pg_atomic_read_u64(&waitLSNState->minWaitedReplayLSN) > currentLSN)
		return;

	wakeupWaiters(WAIT_LSN_REPLAY, currentLSN);
}

/*
 * Wake up processes waiting for flush LSN to reach currentLSN
 */
void
WaitLSNWakeupFlush(XLogRecPtr currentLSN)
{
	/* Fast path check */
	if (pg_atomic_read_u64(&waitLSNState->minWaitedFlushLSN) > currentLSN)
		return;

	wakeupWaiters(WAIT_LSN_FLUSH, currentLSN);
}

/*
 * Clean up LSN waiters for exiting process
 */
void
WaitLSNCleanup(void)
{
	if (waitLSNState)
	{
		/*
		 * We do a fast-path check of the heap flags without the lock.  These
		 * flags are set to true only by the process itself.  So, it's only possible
		 * to get a false positive.  But that will be eliminated by a recheck
		 * inside deleteLSNWaiter().
		 */
		if (waitLSNState->procInfos[MyProcNumber].inReplayHeap)
			deleteLSNWaiter(WAIT_LSN_REPLAY);
		if (waitLSNState->procInfos[MyProcNumber].inFlushHeap)
			deleteLSNWaiter(WAIT_LSN_FLUSH);
	}
}

/*
 * Wait using MyLatch till the given LSN is replayed, the replica gets
 * promoted, or the postmaster dies.
 *
 * Returns WAIT_LSN_RESULT_SUCCESS if target LSN was replayed.
 * Returns WAIT_LSN_RESULT_NOT_IN_RECOVERY if run not in recovery,
 * or replica got promoted before the target LSN replayed.
 */
WaitLSNResult
WaitForLSNReplay(XLogRecPtr targetLSN)
{
	XLogRecPtr	currentLSN;
	int			wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH;

	/* Shouldn't be called when shmem isn't initialized */
	Assert(waitLSNState);

	/* Should have a valid proc number */
	Assert(MyProcNumber >= 0 && MyProcNumber < MaxBackends);

	/*
	 * Add our process to the replay waiters heap.  It might happen that
	 * target LSN gets replayed before we do.  Another check at the beginning
	 * of the loop below prevents the race condition.
	 */
	addLSNWaiter(targetLSN, WAIT_LSN_REPLAY);

	for (;;)
	{
		int			rc;
		currentLSN = GetXLogReplayRecPtr(NULL);

		/* Recheck that recovery is still in-progress */
		if (!RecoveryInProgress())
		{
			/*
			 * Recovery was ended, but recheck if target LSN was already
			 * replayed.  See the comment regarding deleteLSNWaiter() below.
			 */
			deleteLSNWaiter(WAIT_LSN_REPLAY);

			if (PromoteIsTriggered() && targetLSN <= currentLSN)
				return WAIT_LSN_RESULT_SUCCESS;
			return WAIT_LSN_RESULT_NOT_IN_RECOVERY;
		}
		else
		{
			/* Check if the waited LSN has been replayed */
			if (targetLSN <= currentLSN)
				break;
		}

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch, wake_events, -1,
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
	 * Delete our process from the shared memory replay heap.  We might
	 * already be deleted by the startup process.  The 'inReplayHeap' flag prevents
	 * us from the double deletion.
	 */
	deleteLSNWaiter(WAIT_LSN_REPLAY);

	return WAIT_LSN_RESULT_SUCCESS;
}

/*
 * Wait until targetLSN has been flushed on a primary server.
 * Returns only after the condition is satisfied or on FATAL exit.
 */
void
WaitForLSNFlush(XLogRecPtr targetLSN)
{
	XLogRecPtr	currentLSN;
	int			wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH;

	/* Shouldn't be called when shmem isn't initialized */
	Assert(waitLSNState);

	/* Should have a valid proc number */
	Assert(MyProcNumber >= 0 && MyProcNumber < MaxBackends + NUM_AUXILIARY_PROCS);

	/* We can only wait for flush when we are not in recovery */
	Assert(!RecoveryInProgress());

	/* Quick exit if already flushed */
	currentLSN = GetFlushRecPtr(NULL);
	if (targetLSN <= currentLSN)
		return;

	/* Add to flush waiters */
	addLSNWaiter(targetLSN, WAIT_LSN_FLUSH);

	/* Wait loop */
	for (;;)
	{
		int			rc;

		/* Check if the waited LSN has been flushed */
		currentLSN = GetFlushRecPtr(NULL);
		if (targetLSN <= currentLSN)
			break;

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch, wake_events, -1,
					   WAIT_EVENT_WAIT_FOR_WAL_FLUSH);

		/*
		 * Emergency bailout if postmaster has died. This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection due to unexpected postmaster exit"),
					 errcontext("while waiting for LSN flush")));

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	/*
	 * Delete our process from the shared memory flush heap. We might
	 * already be deleted by the waker process. The 'inFlushHeap' flag prevents
	 * us from the double deletion.
	 */
	deleteLSNWaiter(WAIT_LSN_FLUSH);

	return;
}
