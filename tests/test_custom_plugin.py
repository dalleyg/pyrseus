"""
Here we create a custom plugin that lets users trigger an extra shutdown call to
the executor, giving them control over the ``wait`` and ``cancel_futures``
values that are passed to the method.

This plugin demonstrates how to setup and use a ``pre_exit`` function in a
plugin's entry point object.

These tests have some mild race conditions in them because we're trying to
inspect the intermediate states of tasks. If this becomes too problematic, we
could eliminate at least a few of them by forcing more controlled
synchronization between the tasks and the test process. This would be easiest to
do with a threaded executor instead of a multiprocessing one. The known race
conditions can cause false test failures, but they shouldn't cause incorrect
test successes.
"""

import platform
import time
from concurrent.futures import ProcessPoolExecutor
from functools import cached_property

import pytest
from pyrseus.ctx.api import ExecutorPluginEntryPoint, extract_keywords
from pyrseus.ctx.registry import _CONCURRENT_ENTRY_POINTS, register_plugin
from pyrseus.ctx.mgr import ExecutorCtx


def sleep01_and_retV():
    """
    This is the task function we'll be running. It takes a small but non-trivial
    time to run.
    """
    time.sleep(0.1)
    return "V"


EPS = 0.01


def get_abbrev_state(fut):
    """
    Summarizes the state of a Future.
    """
    run = fut.running()
    cancelled = fut.cancelled()
    done = fut.done()
    val = None if not done or cancelled else fut.result(0)
    return {
        # We only enumerate the combinations we actually expect to see in these
        # tests.
        (False, False, False, None): "pending",
        (True, False, False, None): "running",
        (False, True, False, None): "cancel_pending",
        (False, True, True, None): "cancelled",
        (False, False, True, "V"): "success",
    }[run, cancelled, done, val]


def summarize_states(futs):
    return {get_abbrev_state(fut) for fut in futs}


def wait_for_futs(futs, timeout_secs):
    t0 = time.time()
    while time.time() - t0 < timeout_secs:
        summaries = summarize_states(futs)
        if len({"pending", "running"} & summaries) == 0:
            break
        time.sleep(0.1)


@pytest.fixture
def entry_point_and_futs_list():
    futs = []

    class OurEntryPoint(ExecutorPluginEntryPoint):
        supports_serial = False
        supports_concurrent = True
        is_available = True

        @cached_property
        def allowed_keywords(self):
            return extract_keywords(type(self).create) | extract_keywords(
                ProcessPoolExecutor
            )

        def create(
            self,
            max_workers,
            # These two are the features we're adding to this ad-hoc plugin.
            cancel_futures=False,
            wait=False,
            # These are just to make the testing easier.
            states_before_shutdown=None,
            states_after_shutdown=None,
            # Pass through everything else.
            **kwargs,
        ):
            # Do *not* pass the four new arguments to ProcessPoolExecutor. We
            # just need them to be captured by pre_exit in a closure (happens
            # automatically in Python).
            exe = ProcessPoolExecutor(max_workers, **kwargs)

            # This function will be called just before
            # ProcessPoolExecutor.__exit__. The four extra create() arguments
            # are captured in a closure for it to use.
            def pre_exit(exe, exc_info):
                # The asserts are just for this unit test. If we were creating a
                # real plugin, we'd omit them.
                assert summarize_states(futs) == states_before_shutdown
                # This is the extra feature that this ad-hoc plugin gives us.
                if cancel_futures or wait:
                    exe.shutdown(wait=wait, cancel_futures=cancel_futures)
                # Another assert, just for testing.
                assert summarize_states(futs) == states_after_shutdown

            return exe, pre_exit

    assert "altshutdown" not in _CONCURRENT_ENTRY_POINTS
    register_plugin("altshutdown", OurEntryPoint())
    assert "altshutdown" in _CONCURRENT_ENTRY_POINTS
    yield OurEntryPoint(), futs
    del _CONCURRENT_ENTRY_POINTS["altshutdown"]


def test_dont_use_new_feature(entry_point_and_futs_list):
    _, futs = entry_point_and_futs_list
    # Everything works as usual if we disable the new features.
    with ExecutorCtx(
        "altshutdown",
        1,
        cancel_futures=False,
        wait=False,
        states_before_shutdown={"running", "pending"},
        states_after_shutdown={"running", "pending"},  # custom shutdown = noop
    ) as exe:
        t0 = time.time()
        for _ in range(10):
            futs.append(exe.submit(sleep01_and_retV))
    elapsed_secs = time.time() - t0
    assert elapsed_secs >= (0.1 * 10) - EPS  # all of them ran...
    assert summarize_states(futs) == {"success"}  # ...to completion


def test_wait_only_does_nothing_new(entry_point_and_futs_list):
    _, futs = entry_point_and_futs_list
    # Pre-waiting doesn't accomplish anything new; it just makes the waiting
    # happen in our shutdown callback instead of in
    # ProcessPoolExecutor.__exit__.
    with ExecutorCtx(
        "altshutdown",
        1,
        cancel_futures=False,  # the default
        wait=True,  # what __exit__ does
        states_before_shutdown={"running", "pending"},
        states_after_shutdown={"success"},  # resolved earlier
    ) as exe:
        t0 = time.time()
        for _ in range(10):
            futs.append(exe.submit(sleep01_and_retV))
    elapsed_secs = time.time() - t0
    assert elapsed_secs >= (0.1 * 10) - EPS  # all of them ran...
    assert summarize_states(futs) == {"success"}  # ...and are still completed


def test_cancel_without_wait_does_funny_stuff(entry_point_and_futs_list):
    _, futs = entry_point_and_futs_list
    # Cancelling the futures but not waiting does funny things.
    #  - It disables the waiting not just for the explicit shutdown call, but
    #    also permanently disables it for all subsequent calls, including in
    #    ProcessPoolExecutor.__exit__.
    #  - No futures actually get cancelled.
    #  - The worker pool remains alive, even after ProcessPoolExecutor.__exit__
    #    is called. All of the tasks eventually get completed, unprotected by
    #    the context manager.
    #
    # If any of the above-described behavior changes in a future version of
    # Python, we'll need to adjust this test.
    with ExecutorCtx(
        "altshutdown",
        1,
        cancel_futures=True,  # start cancelling
        wait=False,  # this effectively disables the wait in __exit__
        states_before_shutdown={"running", "pending"},  # running + pending
        states_after_shutdown={"running", "pending"},  # cancels not registered yet
    ) as exe:
        t0 = time.time()
        for _ in range(10):
            futs.append(exe.submit(sleep01_and_retV))
    elapsed_secs = time.time() - t0
    assert elapsed_secs < 0.1  # exited quickly
    assert summarize_states(futs) == {"running", "pending"}  # exited too fast
    # everything keeps working till all tasks are done, despite exiting the
    # context
    wait_for_futs(futs, 1.5 if platform.system() == "Linux" else 5)
    assert summarize_states(futs) == {"success"}


def test_cancel_and_wait_works(entry_point_and_futs_list):
    _, futs = entry_point_and_futs_list
    # Cancelling and waiting in the same shutdown call does what we expect: all
    # running tasks are completed, and all pending ones are  permanently
    # cancelled.
    with ExecutorCtx(
        "altshutdown",
        1,
        cancel_futures=True,  # start cancelling
        wait=True,  # but wait until all running ones are done
        states_before_shutdown={"running", "pending"},  # running + pending
        states_after_shutdown={"success", "cancelled"},  # cancelling worked
    ) as exe:
        t0 = time.time()
        for _ in range(10):
            futs.append(exe.submit(sleep01_and_retV))
    t1 = time.time()
    assert 0.01 - EPS <= t1 - t0  # at least 1 should have finished
    # before all would have finished, we should have a combination of success
    # and cancel results
    wait_for_futs(futs, 0.35 if platform.system() == "Linux" else 0.9)
    assert summarize_states(futs) == {"success", "cancelled"}
