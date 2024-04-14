# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # OnError Modes
#
# This notebook discusses how to choose between `pyrseus.ctx.mgr.ExecutorCtx`'s three `OnError` modes for your application.
#
# Its `on_error` parameter controls what `ExecutorCtx.__exit__`
# does with its worker pool and any incomplete futures if an
# "uncaught exception" is flowing through it.

# %% [markdown]
# ## Setup
#
# Before running this notebook, make sure everything it depends on is installed:
#
#     # Modify the docs/requirements.txt path if you're running this
#     # command from anything except the repository's root directory.
#     python -m pip install -r docs/requirements.txt
#
# These are some imports that we'll use throughout this notebook:

# %% tags=["pyflyby-cell"]
import sys

sys.path.append("../../../src")  # assume we're running it from a Pyrseus source clone
import os
import time
from concurrent.futures import ThreadPoolExecutor

from pyrseus.ctx.mgr import ExecutorCtx

# %% [markdown]
# ## "Uncaught Exceptions"
#
# When we say "uncaught exception", we mean an exception that's raised in the
# with-statement and isn't caught and suppressed inside that with-statement. To
# be clear, we're not talking about exceptions inside tasks, unless those
# exceptions are rethrown, e.g. by calling `fut.result()`.
#
# Consider the following function that raises an exception:


# %%
def raises():
    raise RuntimeError("I am an exception!")


# %% [markdown]
# Here is one common source of "uncaught exceptions." The initial exception that
# `raises` raises is caught by the future, but then when we call `fut.result()`,
# we trigger a new exception which we don't catch inside `exe`'s with-statement.

# %%
# NOTE: Almost all of our test cells contain an extra try-except-else construct
# like this one. This is because we're demonstrating what happens with uncaught
# exceptions, but we want to avoid cluttering the output cells with all the
# tracebacks.
try:
    with ThreadPoolExecutor(1) as exe:
        # This statement will cause an exception to be raised in a worker
        # thread. But it'll be caught and bound to the future. We are *not*
        # talking about these cases.
        fut = exe.submit(raises)
        # That said, a common source of uncaught exceptions in an executor's
        # context are tasks' rethrown exceptions when accessing their result()
        # method. We *are* talking about these cases.
        fut.result()

except RuntimeError as ex:
    if not str(ex) == "I am an exception!":
        raise
else:
    raise AssertionError("This example failed.")

# %% [markdown]
#
# Another way to trigger an "uncaught exception" in an executor's
# context is to have some non-future code that does so.

# %%
try:
    with ThreadPoolExecutor(1) as exe:
        # Start a silly task
        fut = exe.submit(raises)
        # Directly trigger an "uncaught exception".
        raise RuntimeError("I am also an exception!")
except RuntimeError as ex:
    assert str(ex) == "I am also an exception!"
else:
    raise AssertionError("This example failed.")

# %% [markdown]
#
# Yet another way is to trigger a `KeyboardInterrupt` while an executor is
# running by pressing the "Interrupt the kernel" button in Jupyter. We don't
# show that here because this is meant to be viewable as a static document.
#
# Next, we'll discuss the three options that Pyrseus supports for dealing
# with these kinds of uncaught exceptions.

# %% [markdown]
# ## Wait: Fail-after Uncaught Exceptions
#
# All of the concurrent `ExecutorCtx` plugins support an  `on_error="wait"`
# mode.
#
# In this mode, the context manager will try to complete every submitted future
# before exiting, even if an uncaught exception is flowing through its
# `__exit__` method. This is useful for fire-and-forget jobs, and for jobs where
# you'll be inspecting each futures' final state after leaving the
# with-statement.
#
# For most underlying executors, this is their default behavior. For example,
# here's implicit `on_error="wait"` behavior in action with the built-in
# `ThreadPoolExecutor` class:

# %%
try:
    with ThreadPoolExecutor(1) as exe:
        # warm up the worker
        assert exe.submit(os.getpid).result() == os.getpid()
        t0 = time.time()
        # submit a slow tasks
        f0 = exe.submit(time.sleep, 0.2)
        # submit a bad task
        f1 = exe.submit(raises)
        # submit another slow tasks
        f2 = exe.submit(time.sleep, 0.2)
        # raise an exception
        raise RuntimeError("uncaught")

except Exception as ex:
    t1 = time.time()
    assert str(ex) == "uncaught", ex  # not "I am en exception!"
    assert 0.4 <= t1 - t0 <= 1.0, t1 - t0  # still ran all futures
    assert f0.exception(0) is None  # f0 is fully done
    assert "I am an exception!" in str(f1.exception(0))  # f1 is fully done
    assert f2.exception(0) is None  # f2 is fully done
else:
    raise AssertionError("This example failed.")

# %% [markdown]
#
# Here's the equivalent code using `ExecutorCtx`. There's nothing surprising
# here.

# %%
try:
    with ExecutorCtx("thread", 1, on_error="wait") as exe:  # <---- THE ONLY CHANGE
        assert exe.submit(os.getpid).result() == os.getpid()
        t0 = time.time()
        f0 = exe.submit(time.sleep, 0.2)
        f1 = exe.submit(raises)
        f2 = exe.submit(time.sleep, 0.2)
        raise RuntimeError("uncaught")
except Exception as ex:
    t1 = time.time()
    if not str(ex) == "uncaught":
        raise
else:
    raise AssertionError("This example failed.")
assert 0.4 <= t1 - t0 <= 1.0, t1 - t0
assert f0.exception(0) is None
assert "I am an exception!" in str(f1.exception(0))
assert f2.exception(0) is None

# %% [markdown]
# ## Cancel: Fail-faster for Uncaught Exceptions
#
# All of the concurrent `ExecutorCtx` plugins except for `ipyparallel` also
# support an `on_error="cancel"` mode.
#
# In this mode, the context manager will try to cancel all of its pending
# futures to provide fail-fast behavior, with two important caveats:
# - Futures whose tasks are already running cannot be cancelled.
# - Many underlying executors will pre-queue some of their futures to reduce
#   dispatcher latency. This makes those pre-queued futures uncancellable too,
#   even though they aren't actually running yet. For example,
#   `ProcessPoolExecutor` pre-queues a single task by default. `loky`'s
#   executors pre-queue many tasks (approximately 2x the number of CPU cores,
#   for various definitions of "cores").
#
# We first demonstrate the cancel behavior without `ExecutorCtx`. Unfortunately,
# it requires some mildly tricky extra boilerplate code. Furthermore, this first
# approach only works with executors that have a `shutdown` method that takes a
# `cancel_futures` argument.

# %%
try:
    with ThreadPoolExecutor(1) as exe:
        # To emulate ExecutorCtx's on_error="cancel" behavior, users can add
        # this extra try-except boilerplate to every with-statement they use
        # with an executor. It works, but it's verbose.
        try:
            assert exe.submit(os.getpid).result() == os.getpid()
            t0 = time.time()
            f0 = exe.submit(time.sleep, 0.2)
            f1 = exe.submit(raises)
            f2 = exe.submit(time.sleep, 0.2)
            # For this test, let's make sure the first task is running so we can
            # see it not get cancelled.
            time.sleep(0.1)
            raise RuntimeError("uncaught")
        except Exception:
            # This approach only works for executors that support a
            # cancel_futures argument to shutdown.
            exe.shutdown(wait=True, cancel_futures=True)
            # Don't forget to reraise the uncaught exception!
            raise

except Exception as ex:
    t1 = time.time()
    assert str(ex) == "uncaught", ex  # since we reraised
    # With ThreadPoolExecutor, there's no pre-queuing, so anything that wasn't
    # actually running at shutdown time is really cancelled. For most executors
    # though, this isn't true: at least one more, and potentially tens to
    # hundreds more futures will be uncancellable and will still start and
    # finish running before the shutdown is done.
    assert 0.2 <= t1 - t0 < 0.4, t1 - t0  # f1 ran but f2 was skipped
    assert f0.exception(0) is None  # f1 ran
    assert f1.cancelled()  # f1 was successfully cancelled
    assert f2.cancelled()  # f2 was successfully cancelled
else:
    raise AssertionError("This example failed.")

# %% [markdown]
#
# Here is a more general way to write the boilerplate when not using
# `ExecutorCtx`. This works with executors whose `shutdown` methods lack a
# `cancel_futures` argument. Note that like all cancel-based approaches, this
# doesn't sidestep the pre-queuing problem.

# %%
try:
    with ThreadPoolExecutor(1) as exe:
        # This try-except construct is a more general way of emulating
        # on_error="cancel". It can be more intrusive, but it doesn't require
        # exe.shutdown to take a cancel_futures argument.
        try:
            assert exe.submit(os.getpid).result() == os.getpid()
            t0 = time.time()
            # With this approach, we need to track all of our futures.
            futs = []
            futs.append(exe.submit(time.sleep, 0.2))
            futs.append(exe.submit(raises))
            futs.append(exe.submit(time.sleep, 0.2))
            time.sleep(0.1)
            raise RuntimeError("uncaught")
        except Exception:
            # This approach works for all executors, assuming that
            # at least some futures are left in a pending state.
            num_cancelled = 0
            for fut in futs:
                num_cancelled += fut.cancel()
            # Don't forget to reraise the uncaught exception!
            raise

except Exception as ex:
    t1 = time.time()
    if not str(ex) == "uncaught":  # since we reraised
        raise
else:
    raise AssertionError("This example failed.")
# See the previous code cell about ThreadPoolExecutor's behavior (tested here)
# vs. how most other executors pre-queue tasks.
assert num_cancelled == 2
assert 0.2 <= t1 - t0 < 0.4, t1 - t0
assert futs[0].exception(0) is None  # f1 ran
assert futs[1].cancelled()  # f1 was successfully cancelled
assert futs[2].cancelled()  # f2 was successfully cancelled

# %% [markdown]
#
# And here's how to get cancel behavior with `ExecutorCtx`, for plugins that
# support it: we just pass `on_error="cancel"` to the constructor. `ExecutorCtx`
# will internally use one of the two above approaches.

# %%
try:
    with ExecutorCtx("thread", 1, on_error="cancel") as exe:
        # No extra try-except needed here. ExecutorCtx does the work for us.
        assert exe.submit(os.getpid).result() == os.getpid()
        t0 = time.time()
        f0 = exe.submit(time.sleep, 0.2)
        f1 = exe.submit(raises)
        f2 = exe.submit(time.sleep, 0.2)
        time.sleep(0.1)
        raise RuntimeError("uncaught")

except Exception as ex:
    t1 = time.time()
    if not str(ex) == "uncaught":
        raise
else:
    raise AssertionError("This example failed.")
assert 0.2 <= t1 - t0 < 0.4, t1 - t0
assert f0.exception(0) is None
assert f1.cancelled()
assert f2.cancelled()

# %% [markdown]
# ## Kill: Fail-fast for Uncaught Exceptions
#
# Currently, only the `loky` plugin for `ExecutorCtx` supports an
# `on_error="kill"` mode.
#
# In this mode, the context manager will kill its workers' processes so that
# users don't have to wait for existing (and pre-queued) tasks to finish running
# before exiting the context. This makes it faster than cancel mode, but this
# may bypass cleanup routines like releasing locks inside the user's task code,
# within-task context managers, atexit handlers in the workers, etc.
#
# Here's a quick example showing how `loky`'s pre-queuing can make the
# previously-discussed cancel mode slow. If you want to make the slowness
# clearer, increase the `time.sleep` argument.

# %%
try:
    with ExecutorCtx("loky", 1, on_error="cancel") as exe:  # <---- CANCEL IS SLOW
        assert exe.submit(os.getpid).result() != os.getpid()
        t0 = time.time()
        # We need to submit a lot of tasks for this demo. Depending on the
        # number of cores available on the current box and whether we're
        # using loky's reusable vs. non-reusable mode, the pre-queue depth
        # can be hundreds of tasks long.
        futs = [exe.submit(time.sleep, 0.2)]
        for _ in range(1000):
            futs.append(exe.submit(os.getpid))
        time.sleep(0.1)
        raise RuntimeError("uncaught")

except Exception as ex:
    t1 = time.time()
    if str(ex) != "uncaught":
        raise
else:
    raise AssertionError("This example failed.")

# The wall time would be longer if the extra pre-queued tasks weren't trivial.
assert 0.2 <= t1 - t0 < 0.4, t1 - t0
assert futs[0].exception(0) is None
# We submitted enough that we'll get a mix between successful and cancelled
# futures.
num_success = 0
num_cancelled = 0
for fut in futs:
    assert fut.done()
    if fut.cancelled():
        num_cancelled += 1
    else:
        assert fut.exception(0) is None
        num_success += 1
# In loky's current implementation, there will always be at least 2 pre-queued
# tasks. Often it's tens or hundreds.
assert num_success >= 3
assert num_cancelled > 0

# %% [markdown]
#
# And here we see that its kill mode is fast. We didn't even wait for the first
# task to be completed to exit the with-statement. If you want to make the
# quickness clearer, increase the `time.sleep` argument and notice that the cell
# still runs just as fast.

# %%
try:
    with ExecutorCtx("loky", 1, on_error="kill") as exe:  # <----- KILL IS FAST
        assert exe.submit(os.getpid).result() != os.getpid()
        t0 = time.time()
        futs = [exe.submit(time.sleep, 0.2)]
        for _ in range(1000):
            futs.append(exe.submit(os.getpid))
        time.sleep(0.1)
        raise RuntimeError("uncaught")

except Exception as ex:
    t1 = time.time()
    if str(ex) != "uncaught":
        raise
else:
    raise AssertionError("This example failed.")

assert 0.1 <= t1 - t0 < 0.2, t1 - t0  # killed even before the first task is done
num_success = 0
num_cancelled = 0
num_exceptions = 0
for fut in futs:
    assert fut.done()
    if fut.cancelled():
        num_cancelled += 1
    elif fut.exception(0) is None:
        num_success += 1
    else:
        num_exceptions += 1
assert num_success == 0  # killed before even the first task is done
assert num_cancelled == 0  # killed, not cancelled
assert num_exceptions == len(futs)  # all killed
