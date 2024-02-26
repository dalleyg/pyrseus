"""
Verifies that `.ExecutorCtx` creates exactly the right number of workers with
the ``"thread"`` and ``"process"`` plugins.
"""

import os
import threading
from contextlib import ExitStack
from multiprocessing import Manager

import pytest

from pyrseus.core.sys import get_num_available_cores
from pyrseus.ctx.mgr import ExecutorCtx
from pyrseus.ctx.registry import skip_if_unavailable

PROC_TIMEOUT_SECS = 1  # should be long enough
THREAD_TIMEOUT_SECS = 0.1  # should be long enough


def coordinated_get_pid_and_tid(parent_pid, parent_tid, barrier, concurrency_style):
    """
    Test helper that ensures that our tasks are all assigned to different
    workers.

    This function blocks until all tasks are also waiting on the `barrier`, then
    fetches the current pid and tid and returns them.
    """
    # Also verify that we're
    if concurrency_style == "serial":
        assert os.getpid() == parent_pid
        assert threading.get_ident() == parent_tid
    elif concurrency_style == "thread":
        assert os.getpid() == parent_pid
        assert threading.get_ident() != parent_tid
    elif concurrency_style == "process":
        assert os.getpid() != parent_pid
    else:
        # If we ever add cross-host testing, then check some relevant host
        # identifier too like the primary IP address.
        raise ValueError(concurrency_style)
    # Wait for all workers to be running this function. This is an easy way to
    # verify how many workers have been started.
    barrier.wait()
    # Tell the main process what this worker's process and thread ids are.
    return os.getpid(), threading.get_ident()


@pytest.mark.parametrize("max_workers", (None, 0, 1, 2))
@pytest.mark.parametrize(
    "plugin,concurrency_style",
    (
        ("cpnocatch", "serial"),
        ("cpinline", "serial"),
        ("cpmpi4py", "process"),
        ("cpprocess", "process"),
        ("nocatch", "serial"),
        ("inline", "serial"),
        ("ipyparallel", "process"),
        ("loky", "process"),
        ("mpi4py", "process"),
        ("pnocatch", "serial"),
        ("pinline", "serial"),
        ("process", "process"),
        ("thread", "thread"),
    ),
)
def test_plugin_pool_size(plugin, concurrency_style, max_workers):
    skip_if_unavailable(plugin)

    if max_workers is None:
        ctx_args = ()
        if concurrency_style == "serial":
            num_tasks = 1
        else:
            # This is the current default for all of the built-in concurrent
            # plugins, except for the mpi4py ones.
            num_tasks = max(1, get_num_available_cores())
    else:
        ctx_args = (max_workers,)
        num_tasks = max(1, max_workers)

    if "mpi4py" in plugin:
        # Problem 1: in its default configuration, mpi4py likes to set slots=1,
        # and it's inconvenient to either override or detect this.
        #
        # Problem 2: mpi4py doesn't play nicely with
        # multiprocessing.Manager.Barrier, at least right out of the box. In
        # particular, it takes some work to securely setup a shared
        # multiprocessing authentication secret. A real MPI user would use
        # MPI-specific mechanisms anyway.
        pytest.xfail("This test is too hard to setup with mpi4py.")
    elif "ipyparallel" in plugin:
        # Problem 1 does *not* apply to ipyparallel. It's fine with
        # oversubscribing CPU resources.
        #
        # Problem 2 also applies to ipyparallel.
        pytest.xfail("This test is too hard to setup with ipyparallel.")

    if concurrency_style == "serial":
        timeout_secs = 1e-9
        if num_tasks > 1:
            pytest.xfail(
                "For serial plugins, this test is only applicable for <2 workers."
            )
    elif concurrency_style == "thread":
        timeout_secs = THREAD_TIMEOUT_SECS
    elif concurrency_style == "process":
        timeout_secs = PROC_TIMEOUT_SECS
    else:
        raise ValueError(concurrency_style)

    parent_pid = os.getpid()
    parent_tid = threading.get_ident()
    with ExitStack() as es:
        # Setup the manager that will create the Barrier object for us.
        sync_mgr = es.enter_context(Manager())

        # Empty for now, but useful to keep for troubleshooting.
        ctx_kwargs = {}

        # Create the ExecutorCtx. It will auto-adjust max_workers if it's zero
        # and the plugin is concurrent-only, or if it's non-zero and the plugin
        # is serial-only.
        ctx = ExecutorCtx(plugin, *ctx_args, **ctx_kwargs)
        exe = es.enter_context(ctx)

        # First check that at least the expected number of workers are started.
        futs = []
        barrier = sync_mgr.Barrier(num_tasks, timeout=timeout_secs)
        for _ in range(num_tasks):
            futs.append(
                exe.submit(
                    coordinated_get_pid_and_tid,
                    parent_pid,
                    parent_tid,
                    barrier,
                    concurrency_style,
                )
            )
        assert len({fut.result() for fut in futs}) == num_tasks

        # Next, verify that no extra concurrent workers were started by making a
        # barrier that's too wide.
        #
        # This is the same test as above, except (a) we use num_tasks+1 when
        # setting up the barrier, and (b) we expect every task to fail instead
        # of none of them.
        futs = []
        barrier = sync_mgr.Barrier(num_tasks + 1, timeout=timeout_secs)
        for _ in range(num_tasks + 1):
            with ExitStack() as es2:
                if plugin.endswith("nocatch"):
                    es2.enter_context(pytest.raises(threading.BrokenBarrierError))
                futs.append(
                    exe.submit(
                        coordinated_get_pid_and_tid,
                        parent_pid,
                        parent_tid,
                        barrier,
                        concurrency_style,
                    )
                )
        for fut in futs:
            # Every future should fail.
            with pytest.raises(threading.BrokenBarrierError):
                fut.result()
