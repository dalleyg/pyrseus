"""
Tests that basic mpi4py-specific features are supported, if that library is
installed.
"""

import os
from functools import cache

import psutil
import pytest

from pyrseus.ctx.mgr import ExecutorCtx
from pyrseus.ctx.registry import is_plugin_available

# Magic global variable that skips the whole test module if the plugin isn't
# installed.
#
# https://docs.pytest.org/en/latest/example/markers.html#scoped-marking
pytestmark = pytest.mark.skipif(
    not is_plugin_available("mpi4py"), reason="mpi4py is not available"
)


@cache
def get_worker_id():
    proc = psutil.Process()
    return (proc.pid, proc.create_time())


def get_inner_func():
    def inner_func():
        return 123

    return inner_func


def test_always_picklable_func():
    # If this crashes, everything will.
    with ExecutorCtx("mpi4py", 1) as exe:
        exe.submit(os.getpid).result()


@pytest.mark.slow
def test_import_env_transferred():
    # This test module sits outside the normal import hierarchy, so this will
    # crash unless the remote workers have replicated the import environment of
    # the main process.
    try:
        with ExecutorCtx("mpi4py", 1) as exe:
            exe.submit(get_worker_id).result()
    except Exception as ex:
        raise AssertionError("failed") from ex


@pytest.mark.slow
def test_inner_func_pickling():
    # If this fails, it means that the pickler didn't figure out that the
    # function isn't at the global scope of its module.
    func = get_inner_func()

    with pytest.raises(Exception):
        with ExecutorCtx("mpi4py", 1) as exe:
            exe.submit(func).result()


@pytest.mark.slow
def test_lambda_pickling():
    # If this fails, it means the pickler can't handle lambdas properly.
    with pytest.raises(Exception):
        with ExecutorCtx("mpi4py", 1) as exe:
            exe.submit(lambda: 123).result()


@pytest.mark.slow
def test_double_picklability():
    # Here we verify that advanced pickling is available in both directions. We
    # send a lambda and get back a lambda.
    with pytest.raises(Exception):
        with ExecutorCtx("mpi4py", 1) as exe:
            inner = lambda: 123  # NOQA
            outer = lambda: inner  # NOQA
            fut = exe.submit(outer)
            fut.result()
