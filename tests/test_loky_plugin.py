"""
Tests that basic loky-specific features are supported, if that library is
installed. Here we mostly test that the ``reusable`` feature functions as we
expect.
"""

import os
import platform
import re
import signal
import time
from contextlib import contextmanager
from functools import cache, partial
from multiprocessing import Manager

import psutil
import pytest

from pyrseus.core.sys import SignalHandlerCtx
from pyrseus.ctx.mgr import ExecutorCtx
from pyrseus.ctx.registry import is_plugin_available

# Magic global variable that skips the whole test module if the plugin isn't
# installed.
#
# https://docs.pytest.org/en/latest/example/markers.html#scoped-marking
pytestmark = pytest.mark.skipif(
    not is_plugin_available("loky"), reason="loky is not available"
)


@cache
def get_worker_id():
    proc = psutil.Process()
    return (proc.pid, proc.create_time())


def get_inner_func():
    def inner_func():
        return 123

    return inner_func


def worker_exists(worker_id):
    pid, create_time = worker_id
    try:
        proc = psutil.Process(pid)
    except Exception:
        return False
    else:
        return create_time == proc.create_time()


def get_abbrev_state(fut):
    """
    Summarizes the state of a Future.
    """
    run = fut.running()
    cancelled = fut.cancelled()
    done = fut.done()
    return {
        # We only enumerate the combinations we actually expect to see in these
        # tests.
        (False, False, False): "pending",
        (True, False, False): "running",
        (False, True, False): "cancel_pending",
        (False, True, True): "cancelled",
        (False, False, True): "success",
    }[run, cancelled, done]


def count_states(futs, sparse=False):
    counts = {
        "pending": 0,
        "running": 0,
        "cancel_pending": 0,
        "cancelled": 0,
        "success": 0,
    }
    for fut in futs:
        state = get_abbrev_state(fut)
        counts[state] += 1
    if sparse:
        counts = {k: v for k, v in counts.items() if v != 0}
    return counts


@pytest.mark.parametrize("reuse", (Ellipsis, False, True, "auto"))
@pytest.mark.parametrize(
    "reusable,expected",
    (
        (Ellipsis, "loky.process_executor.ProcessPoolExecutor"),
        (False, "loky.process_executor.ProcessPoolExecutor"),
        (True, "loky.*ReusablePoolExecutor"),
    ),
)
def test_reusable_vs_reuse_types(reusable, reuse, expected):
    """
    Verifies that the executor type we get back after entering ExecutorCtx is
    exactly what we expect with all combinations of the ``reusable`` and
    ``reuse`` parameters.
    """
    kwargs = {}
    if reusable is not Ellipsis:
        kwargs["reusable"] = reusable
    if reuse is not Ellipsis:
        kwargs["reuse"] = reuse
    with ExecutorCtx("loky", **kwargs) as exe:
        assert exe.__class__.__name__ == "OnExitProxy"
        underlying_cls = exe._exe.__class__
        fqn = f"{underlying_cls.__module__}.{underlying_cls.__name__}"
        assert re.match(expected, fqn), (reusable, reuse, expected, fqn)


def test_reusable_true():
    """
    Verifies that the reusable executors actually get reused.
    """
    with ExecutorCtx("loky", 1, reusable=True) as exe0:
        worker0 = exe0.submit(get_worker_id).result()
        assert get_worker_id() != worker0
    assert worker_exists(worker0)

    with ExecutorCtx("loky", 1, reusable=True) as exe1:
        assert exe1.submit(get_worker_id).result() == worker0
    assert worker_exists(worker0)

    exe1.shutdown(kill_workers=True)  # be a good test and clean up
    if platform.system() == "Windows":
        time.sleep(0.2)  # Windows lacks true SIGTERM, so give it a moment.
    assert not worker_exists(worker0)


def test_reuse_false():
    """
    Verifies that ``reuse=False`` actually causes a new reusable executor to be
    created.
    """
    with ExecutorCtx("loky", 1, reusable=True) as exe0:
        worker0 = exe0.submit(get_worker_id).result()
        assert get_worker_id() != worker0
        assert worker_exists(worker0)
    assert worker_exists(worker0)

    with ExecutorCtx("loky", 1, reusable=True, reuse=False) as exe1:
        assert not worker_exists(worker0)
        worker1 = exe1.submit(get_worker_id).result()
        assert get_worker_id() != worker1
        assert worker0 != worker1
    assert not worker_exists(worker0)
    assert worker_exists(worker1)

    with ExecutorCtx("loky", 1, reusable=True) as exe:
        assert exe.submit(get_worker_id).result() == worker1
    assert not worker_exists(worker0)
    assert worker_exists(worker1)

    exe1.shutdown(kill_workers=True)  # be a good test and clean up
    assert not worker_exists(worker0)
    assert not worker_exists(worker1)


def test_reuse_and_kill_workers():
    """
    Verifies that the ``reuse`` and ``kill_workers`` parameters interact the way
    we expect them to.
    """
    with ExecutorCtx("loky", 1, reusable=True) as exe0:
        worker0 = exe0.submit(get_worker_id).result()
        assert get_worker_id() != worker0
    assert worker_exists(worker0)

    # kill_workers is ignored when reuse=True
    with ExecutorCtx("loky", 1, reusable=True, reuse=True, kill_workers=True) as exe1:
        assert exe1.submit(get_worker_id).result() == worker0
    assert worker_exists(worker0)

    # but it does bounce the workers when reuse=False
    with ExecutorCtx("loky", 1, reusable=True, reuse=False, kill_workers=True) as exe2:
        worker2 = exe2.submit(get_worker_id).result()
        assert get_worker_id() != worker2
        assert worker0 != worker2
    assert not worker_exists(worker0)
    assert worker_exists(worker2)

    with ExecutorCtx("loky", 1, reusable=True) as exe3:
        assert exe3.submit(get_worker_id).result() == worker2
    assert not worker_exists(worker0)
    assert worker_exists(worker2)

    exe3.shutdown(kill_workers=True)  # be a good test and clean up
    if platform.system() == "Windows":
        time.sleep(0.2)  # Windows lacks true SIGTERM, so give it a moment.
    assert not worker_exists(worker0)
    assert not worker_exists(worker2)


def test_always_picklable_func():
    """
    Test that verifies that if our other picklability tests fail, it's due to a
    general problem, not a pickling-specific problem.
    """
    # If this crashes, everything will.
    with ExecutorCtx("loky", 1) as exe:
        exe.submit(os.getpid).result()


def test_import_env_transferred():
    """
    Verifies that our `sys.path` gets propagated to the workers with the
    ``"loky"`` plugin. If this fails, then the rest of the pickling tests may
    be failing for a more general reason that the specific picklability testing
    they're trying to do.
    """
    # This test module sits outside the normal import hierarchy, so this will
    # crash unless the remote workers have replicated the import environment of
    # the main process.
    with ExecutorCtx("loky", 1) as exe:
        exe.submit(get_worker_id).result()


def test_inner_func_pickling():
    """
    Verifies that non-global functions get pickled correctly. These have the
    same type as global functions but are not directly importable.
    """
    # If this fails, it means that the pickler didn't figure out that the
    # function isn't at the global scope of its module.
    with ExecutorCtx("loky", 1) as exe:
        func = get_inner_func()
        assert exe.submit(func).result() == 123


def test_lambda_pickling():
    """
    Verifies that lambda functions get pickled correctly. These have a different
    type than global or inner functions, so it's generally easier to get these
    working properly than global functions. E.g. these can sometimes work even
    if `sys.path` has not been propagated to the workers.
    """
    # If this fails, it means the pickler can't handle lambdas properly.
    with ExecutorCtx("loky", 1) as exe:
        assert exe.submit(lambda: 123).result() == 123
        assert exe.submit(lambda: get_inner_func()()).result() == 123
        func = get_inner_func()
        assert exe.submit(lambda: func()).result() == 123
        assert exe.submit(lambda func=func: func()).result() == 123


def test_double_picklability():
    """
    Verifies that a smart pickler like |cloudpickle|_ is being used not just for
    sending tasks, but also for getting their results.
    """
    # Here we verify that advanced pickling is available in both directions. We
    # send a lambda and get back a lambda.
    with ExecutorCtx("loky", 1) as exe:
        inner = lambda: 123  # NOQA
        outer = lambda: inner  # NOQA
        fut = exe.submit(outer)
        inner_ret = fut.result()
        ret = inner_ret()
        assert ret == 123


@contextmanager
def setup_reusable_test(max_workers):
    """
    Helper for tests that mix a reusable executor, a managed proxy of that same
    executor, and the ability to create shared barriers for testing.
    """
    from loky import get_reusable_executor

    kw = {"max_workers": max_workers, "timeout": 60}
    get_reusable = partial(get_reusable_executor, **kw)
    get_ctx = partial(ExecutorCtx, "loky", reusable=True, **kw)

    exe = None
    try:
        with Manager() as sync_mgr:
            yield get_reusable, get_ctx, sync_mgr.Barrier
    finally:
        if exe is not None:
            exe.shutdown(wait=True, kill_workers=True)


def test_reusable_can_exit_ctx_while_direct_fut_blocks():
    """
    Test for using a reusable executor both directly and via the context
    manager: verifies that we can exit the context while a direct fut is still
    running.
    """
    # Future work: troubleshoot this. Guess: this might be related to the
    # problem in test_ipyparallel_plugin's test_simple_args test on macOS.
    if platform.system() == "Darwin":
        pytest.skip("This test times out on macOS.")

    barrier_sets = []

    def get_next_waiter(*barriers):
        """
        Appends the supplied `barriers` as a tuple to `barrier_sets` and returns
        a lambda that will wait on that just-added set.
        """
        barrier_sets.append(barriers)
        return lambda: len([b.wait() for b in barriers])

    def get_num_waiting(num_waiters_to_ignore=0):
        """
        The number of barrier sets that have a least one waiting party, after
        taking into account ``num_waiters_to_ignore``.
        """
        num_waiting = 0
        for barriers in barrier_sets:
            if any(barrier.n_waiting > num_waiters_to_ignore for barrier in barriers):
                num_waiting += 1
        return num_waiting

    with setup_reusable_test(2) as (get_reusable, get_ctx, Barrier):
        # Request 2 workers.
        direct = get_reusable()

        # Hack to make the queue be small enough that all of our submitted tasks
        # don't get immediately slurped up and made uncancellable.
        super(type(direct), direct)._setup_queues(None, None, queue_size=1)

        # Submit a direct task that'll block on two barriers. The first will be
        # shared with the first ctx task to help us verify that Loky uses FIFO
        # scheduling. The second is used to control when this particular task is
        # allowed to finish.
        futs = [direct.submit(get_next_waiter(Barrier(3), Barrier(2)))]

        # Now create a context manager that reuses the same executor.
        with get_ctx() as ctx:
            try:
                # Succeeds because of the way we setup get_reusable and get_ctx'
                # arguments.
                assert ctx._exe is direct

                # Append one task that'll jointly wait with the direct task.
                futs.append(ctx.submit(get_next_waiter(barrier_sets[0][0], Barrier(2))))

                # Append a few more that should all be stuck behind the first
                # two. Make enough of them that we can successfully cancel some
                # later, despite Loky's aggressive latency-optimized queueing
                # system.
                for i in range(2, 100):
                    futs.append(ctx.submit(get_next_waiter(Barrier(2))))
                assert len(barrier_sets) == 100
                assert len(futs) == 100

                # Wait for the first two to be running. Note that we can't trust
                # the abbrev_state too much: Loky puts extra tasks in the
                # running state to reduce latency, even though they're not
                # actually running yet.
                assert barrier_sets[0][0] is barrier_sets[1][0]
                barrier_sets[0][0].wait(5)
                time.sleep(0.01)  # give both workers a moment to hit the next barrier
                assert get_abbrev_state(futs[0]) == "running"
                assert get_abbrev_state(futs[1]) == "running"
                assert barrier_sets[0][0].n_waiting == 0  # passed shared barrier
                assert barrier_sets[0][1].n_waiting == 1  # waiting on our signal
                assert barrier_sets[1][1].n_waiting == 1  # waiting on our signal
                assert get_num_waiting() == 2  # only the first 2 tasks waiting

                # Now let's try to cancel all of the actually-pending tasks so
                # that we don't deadlock the exit. Note that we *don't* wait on
                # their barriers. We want to verify that they get cancelled
                # before being scheduled.
                num_cancelled = 0
                for i in range(2, 100):
                    num_cancelled += futs[i].cancel()
                assert num_cancelled == 100 - 3  # 2 in progress, 1 pre-queued

                # Let fut 1 (only) return. We have to give a small timeout here
                # because we're doing sequential waits in the workers, not set
                # waits.
                barrier_sets[1][1].wait(1)
                assert futs[1].result() == 2  # task had 2 barriers

                # We also need to let the one pre-queued task also proceed since
                # we couldn't cancel it.
                barrier_sets[2][0].wait(5)
                assert futs[2].result() == 1  # task had 1 barrier

                # Exit the context.

            except BaseException:
                # If something in this test fails, immediately shut down
                # everything instead of being stuck in a deadlock we may have
                # created with our barriers.
                direct.shutdown(wait=True, kill_workers=True)
                raise

        # Immediately after exiting the context, we should be able to check the
        # states. There should be no intermediate states due to the wait call in
        # the context's __exit__.
        assert get_abbrev_state(futs[0]) == "running"
        assert get_abbrev_state(futs[1]) == "success"
        assert get_abbrev_state(futs[2]) == "success"
        counts = count_states(futs, sparse=True)
        assert counts == {"running": 1, "success": 2, "cancelled": 97}

        # Let the direct task finish.
        barrier_sets[0][1].wait(1)
        for _ in range(10):
            if get_abbrev_state(futs[0]) == "success":
                break
            time.sleep(0.1)
        assert get_abbrev_state(futs[0]) == "success"
        counts = count_states(futs, sparse=True)
        assert counts == {"success": 3, "cancelled": 97}

        direct.shutdown(wait=True, kill_workers=True)


def test_reusable_futs_wait_on_exit():
    """
    This is a companion test to test_can_exit_ctx_while_direct_fut_blocks. That
    one verified that __exit__ doesn't block on futures associated with the
    reusable executor but not with the ExecutorCtx. This one tests that __exit__
    does block on futures created by the ExecutorCtx.
    """
    if platform.system() == "Windows":
        pytest.skip("This test needs SIGALRM, but it is unavailable on Windows.")

    with setup_reusable_test(2) as (get_reusable, get_ctx, Barrier):
        # Here we're setting up a barrier that lets us control when our test
        # task is allowed to finish. We also setup an alarm system that lets us
        # release the barrier asynchronously.
        barrier = Barrier(2)
        barrier_state = "created"  # for troubleshooting

        def release_barrier_on_sigalrm(signum, frame):
            if signum == signal.SIGALRM:
                nonlocal barrier_state
                barrier_state = "waiting"
                barrier.wait()
                barrier_state = "released"

        with SignalHandlerCtx(signal.SIGALRM, release_barrier_on_sigalrm):
            with get_ctx() as exe:
                # Warm up the system.
                assert None is exe.submit(lambda: None).result()

                # Estimate the round trip time.
                t0 = time.time()
                assert None is exe.submit(lambda: None).result()
                t1 = time.time()
                rtt_secs = t1 - t0

                # Submit a task that'll block till we release it. But don't wait
                # on
                fut = exe.submit(barrier.wait)

                # Setup the alarm for a little while in the future. Make it long
                # enough that it's unlikely that the task would take that long
                # if we used a 1-party instead of 2-party barrier.
                wait_secs = max(1, int(rtt_secs * 4))
                earliest_exit_time = time.time() + wait_secs
                assert barrier_state == "created"
                signal.alarm(wait_secs)

            # Verify that __exit__ blocked till our task was done running.
            actual_exit_time = time.time()
            assert actual_exit_time >= earliest_exit_time
            assert barrier_state == "released"
            assert get_abbrev_state(fut) == "success"


def test_reusable_fut_leaks():
    """
    Verifies that in reusable mode, saved futures are quickly removed from the
    set of ones we save for __exit__-time cleanup.

    This is a whitebox text because it's a lot easier to write it that way, and
    we're testing an internal optimization.
    """
    with setup_reusable_test(1) as (get_reusable, get_ctx, Barrier):
        with get_ctx() as exe:
            # Submit a bunch of tasks that each block tell we let them proceed.
            barriers = []
            futs = []
            for _ in range(10):
                barriers.append(Barrier(2))
                futs.append(exe.submit(barriers[-1].wait))
            # All of them are in the cleanup set.
            assert exe._futs == set(futs)
            # Release the futures one at a time and verify that they get removed
            # from the cleanup set before we get them back.
            for i in range(10):
                assert exe._futs == set(futs[i:])
                barriers[i].wait(5)
                futs[i].result()
            assert len(exe._futs) == 0
