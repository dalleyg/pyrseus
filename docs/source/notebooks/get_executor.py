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
# # `ExecutorCtx` vs. `get_executor`
#
# This notebook discusses how to use both `ExecutorCtx`
# and `get_executor` for the most common situations.

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
from concurrent.futures import as_completed
from contextlib import ExitStack

from pyrseus.ctx.mgr import ExecutorCtx
from pyrseus.interactive import get_executor

# %% [markdown]
# And here are some helper functions we'll be using throughout this notebook:


# %%
def get_row(r):
    """
    Returns the requested row from a 10x10 matrix with 0:10
    on the first row, 10:20 on the second row, etc.
    """
    if not (0 <= r < 10):
        raise IndexError(r)
    return list(range(r * 10, r * 10 + 10))


def get_col(c):
    """
    Returns the requested column from a 10x10 matrix with 0:10
    on the first row, 10:20 on the second row, etc.
    """
    if not (0 <= c < 10):
        raise IndexError(c)
    return list(range(c, 100, 10))


# %% [markdown]
# ## A Single Set of Tasks
#
# If your script or notebook has a single concurrent block, then `ExecutorCtx`
# can be a safe and convenient helper. Pick whatever plugin makes the most sense
# for your problem.
#
# If we want all futures to be attempted, even if some earlier ones fail, we can
# do this:

# %%
with ExecutorCtx("loky", 2) as exe:
    futs = [exe.submit(lambda r=r: sum(get_row(r))) for r in range(10)]
# With the fut.result() calls outside the with-statement, exe will keep
# trying to run every future, even if some fail.
print([fut.result() for fut in futs])

# %% [markdown]
# Or if we want to kill all workers immediately on the first detected error, we
# can check the results inside the with-statement. This optional feature is one
# of `ExecutorCtx`'s value adds (currently only supported with the `loky`
# plugin; see the `on_error` notebook for details):

# %%
with ExecutorCtx("loky", 2) as exe:
    futs = [exe.submit(lambda r=r: sum(get_row(r))) for r in range(10)]
    # With the fut.result() calls inside the with-statement, most
    # plugins will try to stop running additional futures if it can.
    print([fut.result() for fut in futs])

# %% [markdown]
# ## Multiple Sets of Tasks
#
# If your script or notebook needs to run multiple separate sets of tasks, there
# are several approaches to choose from.

# %% [markdown]
# ### ExecutorCtx with reusable loky executors
#
# Our `loky` plugin supports their reusable executors. Those executors try to
# reuse the existing worker pool from the previous time a reusable executor was
# requested. `ExecutorCtx` lets users combine `loky`'s reusable executors with
# the safety that context managers give us, including our choice of `on_error`
# handling (see the notebook of the same name for details).

# %%
# Do some work in a cell with an executor.
with ExecutorCtx("loky", 1, reusable=True) as exe:
    cell_1_worker_pid = exe.submit(os.getpid).result()
    futs = [exe.submit(lambda r=r: sum(get_row(r)), r) for r in range(10)]
    row_sums = [fut.result() for fut in futs]
    print(row_sums)
    print(sum(row_sums))

# %%
# And now we can do some more work in another cell, with another context
# manager.
with ExecutorCtx("loky", 1, reusable=True) as exe:
    cell_2_worker_pid = exe.submit(os.getpid).result()
    futs = [exe.submit(lambda c=c: sum(get_col(c)), c) for c in range(10)]
    col_sums = [fut.result() for fut in futs]
    print(col_sums)
    print(sum(col_sums))

# %%
# Both context managers used the same worker process.
assert cell_1_worker_pid == cell_2_worker_pid


# %% [markdown]
# ### Driver Functions that Take an Executor
#
# When writing library code, sometimes one has driver functions that want to
# run many concurrent tasks. If the author wants to support using an
# externally-supplied executor, they can have it passed in to the driver
# function or class instead of creating it themselves.
#
# Here's an example where we have two driver functions that we run with a single
# executor. This works with any executor, but it can requires work to avoid
# crosstalk between the drivers.


# %%
# This would typically be a library function.
def my_row_driver(exe):
    # Here we take an existing executor, submit tasks to it and track our tasks'
    # futures. We *don't* enter or exit the executor's context.
    #
    # Submit a bunch of tasks, including one that has a bad input (r=10).
    futs = []
    for r in range(11):
        futs.append(exe.submit(lambda r=r: sum(get_row(r)), r))
    # While we're at it, here we show a way to (a) use as_completed, (b) collate
    # results, and (c) save any task exceptions.
    #
    # This extra work is one of the downsides of this approach.
    row_sums = [None] * len(futs)
    for fut in as_completed(futs):
        r = futs.index(fut)
        try:
            row_sums[r] = fut.result()
        except Exception as ex:
            row_sums[r] = ex
    return row_sums


# This might be another library function.
def my_col_driver(exe):
    # As with my_row_driver, we'll intentionally submit one bad task (with c=10).
    futs = [exe.submit(lambda c=c: sum(get_col(c)), c) for c in range(11)]
    # Here we'll let the whole driver fail if there were any task errors.
    # But we'll at least be nice and cancel any remaining futures.
    #
    # This extra work is one of the downsides of this approach.
    col_sums = []
    for c in range(len(futs)):
        try:
            col_sums.append(futs[c].result())
        except Exception:
            # Try to cancel all remaining tasks. This is a noop for any
            # tasks that are already done.
            for c in range(c + 1, len(futs)):
                futs[c].cancel()
            # And reraise the exception
            raise
    return col_sums


# %%
# And this might be in our script or notebook where we want to run both drivers
# with an executor that we created and own.
try:
    with ExecutorCtx("thread", 1) as exe:
        print(f"row sums: {my_row_driver(exe)}")
        print(f"col sums: {my_col_driver(exe)}")
except Exception as ex:
    print(f"Caught {ex!r}")


# %% [markdown]
# ### Dual Use Driver Functions with ExitStack
#
# Another common situation is to have a driver function that can work in either
# the "A Single Set of Tasks" or the "Driver Functions that Take an Executor"
# way. The `ExitStack` context manager can make these dual-use driver functions
# easier to write.


# %%
# Here's a minimal example of a dual-use driver.
def my_dual_use_row_driver(exe=None):
    # First, we create a context manager that can be used to manage other
    # context managers, including none at all.
    with ExitStack() as es:
        # Now we conditionally create an executor and enter its context.
        if exe is None:
            exe = es.enter_context(ExecutorCtx("thread", 1))
        # And now we can proceed as usual.
        futs = [exe.submit(lambda r=r: sum(get_row(r)), r) for r in range(10)]
        # To make this safe, we'll cancel futures on error.
        #
        # This extra work is one of the downsides of this approach.
        try:
            return [fut.result() for fut in futs]
        except Exception:
            for fut in futs:
                fut.cancel()  # auto-ignored if it's already done
            raise


# %%
# Letting it create and manage its own executor.
my_dual_use_row_driver()

# %%
# Using our executor, managed by us.
with ExecutorCtx("cpprocess", 1) as exe:  # loky would be fine too
    print(my_dual_use_row_driver(exe=exe))

# %% [markdown]
# ### get_executor
#
# A final approach is to forego context managers completely and use
# `get_executor`. This saves the trouble of using a context manager, but it also
# removes all of the protections it gives, including `ExecutorCtx`'s
# enhancements to `loky`'s reusable executor.
#
# The benefits of using `get_executor` in Jupyter notebooks include:
#  - There's no need to create and use a context manager in each cell that
#    submits tasks.
#  - It can be convenient when using exiting "Driver Functions that Take an
#    Executor", as long as they're written properly to clean up after themselves
#    if they raise an exception.
#  - It allows reusing the same executor across many cells, for any plugin, not
#    just the `loky` one.
#  - It helps bypass long startup times for some plugins like `ipyparallel`.
#
# The downsides include:
#  - Users must remember to block on every future instead of letting the context
#    manager do that automatically and implicitly at ``__exit__`` time.
#  - If a cell that's managing futures is interrupted, submitted tasks will
#    continue to run in the background, potentially creating a silent backlog
#    for new tasks submitted afterward.
#  - Finalizers are not reliable for resource management in Python, so the
#    wrapper's destructor may never be triggered. This can lead to resource
#    leaks: tasks continuing to run when the user didn't intend them to, workers
#    not being torn down, worker management threads or processes not being torn
#    down, etc.
#
# Here is an example of using it with the dual-use driver from the previous
# section:

# %%
print(my_dual_use_row_driver(exe=get_executor("cpprocess", 1)))
