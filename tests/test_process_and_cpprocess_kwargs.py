"""
Tests various pickling and kwarg features of both `.ProcessPoolExecutor` and
`.CpProcessPoolExecutor`. This test case requires pytest or a compatible runner
due to the test parametrization we use.
"""

import inspect
import multiprocessing
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from pickle import PickleError
from unittest import SkipTest

import pytest
from pyrseus.core.sys import is_mp_start_method_supported
from pyrseus.executors.cpprocess import CpProcessPoolExecutor

POOL_PARAMS = inspect.signature(ProcessPoolExecutor).parameters
"""
Parameters of the underlying ProcessPoolExecutor, for detecting features that
only exist in some versions of Python and/or on some platforms.
"""

PID = os.getpid()


def get_worker_id():
    return os.getpid()


def get_global_pid():
    return PID


def init_global_pid(pid):
    global PID
    PID = pid


def get_lambda():
    return lambda: 123


def _get_mp_context_or_skip(start_method):
    if start_method is None:
        mp_context = None
    elif "mp_context" not in POOL_PARAMS:
        raise SkipTest("mp_context is not supported in your interpreter")
    elif not is_mp_start_method_supported(start_method):
        raise SkipTest(f"{start_method=} is not supported in your interpreter")
    else:
        mp_context = multiprocessing.get_context(start_method)
    return mp_context


@pytest.mark.parametrize("start_method", (None, "fork", "forkserver", "spawn"))
def test_start_methods(start_method):
    # Check that our initial state is reasonable.
    fake_worker_id = -123
    assert get_worker_id() != fake_worker_id
    assert PID != fake_worker_id
    assert PID == os.getpid()

    # Create the mp_context.
    mp_context = _get_mp_context_or_skip(start_method)

    # Verify that the initializer ran for the worker process.
    with CpProcessPoolExecutor(
        1,
        mp_context=mp_context,
        initializer=init_global_pid,
        initargs=(fake_worker_id,),
    ) as exe:
        assert exe.submit(get_global_pid).result() == fake_worker_id

    # Make sure it didn't poison our process.
    assert PID != fake_worker_id

    # Make sure it didn't poison future workers.
    with CpProcessPoolExecutor(1, mp_context=mp_context) as exe:
        assert exe.submit(get_global_pid).result() != fake_worker_id

    # Ensure we didn't pollute and globals in this process.
    assert PID == os.getpid()


def test_max_tasks_per_child():
    if "max_tasks_per_child" not in POOL_PARAMS:
        raise SkipTest("max_tasks_per_child is not supported in your interpreter")
    # NOTE: until https://github.com/python/cpython/issues/115831 is resolved,
    # only use NUM_TASKS_PER_SET=1. Larger values will hang or crash the
    # process, due to bugs in the multiprocessing library.
    NUM_SETS = 5
    NUM_TASKS_PER_SET = 1
    with CpProcessPoolExecutor(max_tasks_per_child=NUM_TASKS_PER_SET) as exe:
        futs = [exe.submit(get_worker_id) for _ in range(NUM_SETS * NUM_TASKS_PER_SET)]
        counts = defaultdict(int)
        for fut in futs:
            counts[fut.result()] += 1
        assert len(counts) == NUM_SETS
        for pid, count in counts.items():
            assert count == NUM_TASKS_PER_SET, (pid, count)


@pytest.mark.parametrize("start_method", (None, "fork", "forkserver", "spawn"))
@pytest.mark.parametrize(
    "cls,should_succeed,func",
    (
        (ProcessPoolExecutor, True, get_worker_id),
        (ProcessPoolExecutor, False, lambda: get_worker_id),
        (ProcessPoolExecutor, False, get_lambda),
        (CpProcessPoolExecutor, True, get_worker_id),
        (CpProcessPoolExecutor, True, lambda: get_worker_id),
        (CpProcessPoolExecutor, True, get_lambda),
    ),
)
def test_pickling(cls, should_succeed, func, start_method):
    """
    Verifies that

     - ProcessPoolExecutor can only pickle normally-picklable things (mostly to
       show that the CpProcessPoolExecutor tests are actually testing something
       real

     - CpProcessPoolExecutor can handle bi-directional pickling that requires
       cloudpickle.
    """
    mp_context = _get_mp_context_or_skip(start_method)
    with ExitStack() as es:
        exe = es.enter_context(cls(1, mp_context=mp_context))
        if not should_succeed:
            # Trap errors from the library, not assertions from this test.
            es.enter_context(pytest.raises((AttributeError, RuntimeError, PickleError)))
        fut = exe.submit(func)
        ret = fut.result()
        if func is get_lambda:
            assert ret() == 123
        else:
            assert ret != get_worker_id()
