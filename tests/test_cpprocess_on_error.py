"""
Verifies that `.CpProcessPoolExecutor` handles all of its ``on_error`` modes
correctly.
"""

import os
import platform
import signal
import sys
import time
from concurrent.futures import CancelledError, TimeoutError
from concurrent.futures.process import EXTRA_QUEUED_CALLS
from multiprocessing import Manager

import pytest
from pyrseus import CpProcessPoolExecutor
from pyrseus.core.sys import SignalHandlerCtx

# These tests rely on SIGALRM. Unfortunately, that doesn't exist on Windows.
pytestmark = pytest.mark.skipif(
    platform.system() == "Windows", reason="SIGALRM does not exist on Windows"
)


def get_abbrev_state(fut):
    """
    Summarizes the state of a Future.
    """
    run = fut.running()
    done = fut.done()
    cancelled = False
    has_exc = False
    try:
        has_exc = fut.exception(0) is not None
    except TimeoutError:
        pass
    except CancelledError:
        cancelled = True
    return {
        # We only enumerate the combinations we actually expect to see in these
        # tests.
        (False, False, False, False): "pending",
        (True, False, False, False): "running",
        (False, True, False, False): "cancel_pending",
        (False, True, True, False): "cancelled",
        (False, False, True, False): "success",
        (True, False, False, True): "exception_pending",
        (False, False, True, True): "exception",
    }[run, cancelled, done, has_exc]


def test_always_wait_if_no_error():
    """
    Tests that CpProcessPoolExecutor always waits till all submitted tasks enter
    a final state before exiting when there are no uncaught exceptions.
    """

    # How many extra tasks concurrent.futures pre-queues to lower latency. These
    # can't be cancelled.
    assert 0 <= EXTRA_QUEUED_CALLS < 2

    # Called asynchronously 1s after all futures have been submitted.
    def release_barriers_on_sigalrm(signum, frame):
        assert signum == signal.SIGALRM
        # Verify that exactly 1 task is really running and is now waiting on our
        # barrier.
        for i, barrier in enumerate(barriers):
            if i == 0:
                assert barrier.n_waiting == 1
            else:
                assert barrier.n_waiting == 0
        # Meet each task at our barrier. We include a small scheduling timeout.
        for barrier in barriers:
            barrier.wait(0.1)

    # Submit some tasks that wait on a 1s alarm timer before being allowed to
    # proceed. While the timer is waiting, start exiting the context. Verify
    # that (a) ctx.__exit__ blocks until all futs are done, but (b) it's
    # otherwise running pretty quickly [otherwise we have to doubt our min
    # bound].
    with Manager() as sync_mgr:
        ctx = CpProcessPoolExecutor(1)
        with SignalHandlerCtx(signal.SIGALRM, release_barriers_on_sigalrm):
            try:
                exe = ctx.__enter__()
                assert exe.submit(os.getpid).result() != os.getpid()  # warm up
                # Create one really-started task, fill the rest of the task
                # pre-queue, and create one really not started task.
                barriers = []
                futs = []
                for _ in range(1 + EXTRA_QUEUED_CALLS + 1):
                    barriers.append(sync_mgr.Barrier(2))
                    futs.append(exe.submit(barriers[-1].wait))
                t0 = time.time()
                signal.alarm(1)
            except:  # NOQA
                exc_info = sys.exc_info()
                ctx.__exit__(*exc_info)
                raise AssertionError("Exception not expected") from exc_info[1]
            else:
                ctx.__exit__(None, None, None)
            t1 = time.time()
            for fut in futs:
                state = get_abbrev_state(fut)
                if state == "exception":
                    exc = RuntimeError("Unexpected task exception")
                    raise exc from fut.exception()
                else:
                    assert state == "success"
            elapsed_secs = t1 - t0
            # Should block on the 1s alarm, but then finish all tasks quickly.
            assert 1.0 <= elapsed_secs <= 1.3


def test_wait_on_error():
    """
    Tests that CpProcessPoolExecutor actually waits on all submitted futures
    when there's an uncaught exception.
    """

    # This is similar to test_always_wait_if_no_error, except we raise an
    # exception right after we start releasing the barriers. Like it, this
    # should wait till all tasks are done. For clarity, the only other comments
    # in this test function are ones that highlight which lines differ from
    # test_always_wait_if_no_error's.

    assert 0 <= EXTRA_QUEUED_CALLS < 2

    def release_barriers_on_sigalrm(signum, frame):
        assert signum == signal.SIGALRM
        for i, barrier in enumerate(barriers):
            if i == 0:
                assert barrier.n_waiting == 1
            else:
                assert barrier.n_waiting == 0
        for barrier in barriers:
            barrier.wait(0.1)

    with Manager() as sync_mgr:
        ctx = CpProcessPoolExecutor(1)
        with SignalHandlerCtx(signal.SIGALRM, release_barriers_on_sigalrm):
            try:
                exe = ctx.__enter__()
                assert exe.submit(os.getpid).result() != os.getpid()
                barriers = []
                futs = []
                for _ in range(1 + EXTRA_QUEUED_CALLS + 1):
                    barriers.append(sync_mgr.Barrier(2))
                    futs.append(exe.submit(barriers[-1].wait))
                t0 = time.time()
                signal.alarm(1)  # <------------------------------------ DIFFERS
                exc = RuntimeError("start exiting early")  # <---------- DIFFERS
                raise exc  # <------------------------------------------ DIFFERS
            except:  # NOQA
                exc_info = sys.exc_info()
                ctx.__exit__(*exc_info)
                assert exc_info[1] is exc, exc_info[
                    1
                ]  # <---------------------------DIFFERS
            t1 = time.time()
            for fut in futs:
                state = get_abbrev_state(fut)
                if state == "exception":
                    exc = RuntimeError("Unexpected task exception")
                    raise exc from fut.exception()
                else:
                    assert state == "success"
            elapsed_secs = t1 - t0
            assert 1.0 <= elapsed_secs <= 1.3
