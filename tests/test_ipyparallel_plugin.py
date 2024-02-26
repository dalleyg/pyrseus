"""
Tests that basic ipyparallel-specific features are supported, if that library is
installed.
"""

import logging
import os
import platform
import time
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
    not is_plugin_available("ipyparallel"), reason="ipyparallel is not available"
)

THIS_PROCESS = psutil.Process()


@cache
def get_worker_id():
    proc = psutil.Process()
    return (proc.pid, proc.create_time())


def get_inner_func():
    def inner_func():
        return 123

    return inner_func


def get_children():
    ret = set()
    for child in THIS_PROCESS.children():
        if "cov-report" in " ".join(child.cmdline()):
            # If running coverage tests, ignore its background processes that
            # come and go.
            continue
        try:
            # The pid and creation time are for uniqueness. The cmdline is for
            # troubleshooting.
            ret.add((child.pid, child.create_time(), " ".join(child.cmdline())))
        except psutil.NoSuchProcess:
            pass
    return ret


def test_simple_args():
    # Future work: troubleshoot why initial_children has an extra
    # multiprocessing.spawn.spawn_main process that disappears by the time we
    # hit exe_children. This only happens on macOS. Guess: ipyparallel is
    # changing the worker start mode so multiprocess kills a helper process for
    # the previous mode.
    if platform.system() == "Darwin":
        pytest.xfail("This test has spurious failures on macOS")

    # It's fine for some child processes to exist, as long as nothing else
    # changes that set within this test.
    initial_children = get_children()
    # Use ipyparallel to do some work.
    with ExecutorCtx("ipyparallel", 1) as exe:
        exe_children = get_children()
        # If something else in this test process is concurrently spawning or
        # reaping processes, it'll confuse our test.
        assert exe_children > initial_children
        # It should have started up 1 controller (fixed) and 1 engine (per our
        # request). This may need adjusting if they ever redesign their system,
        # e.g. add some kind of nanny process process of this one.
        assert len(exe_children) == len(initial_children) + 2
        # Run something trivial.
        assert exe.submit(os.getpid).result() != os.getpid()
        # We should still have the same children.
        assert exe_children == get_children()
    # Ensure it cleaned up after itself.
    assert initial_children == get_children()


@pytest.mark.slow
def test_cluster_provided():
    import ipyparallel as ipp

    initial_children = get_children()
    cluster = ipp.Cluster(n=1, log_level=logging.FATAL)
    try:
        # It should not auto-start anything yet. For this part, we give it a
        # little time since it takes a while to create and connect to its
        # subprocesses.
        time.sleep(2)
        assert initial_children == get_children()
        # Now trigger things normally.
        with ExecutorCtx("ipyparallel", cluster=cluster) as exe:
            exe_children = get_children()
            assert exe_children > initial_children
            assert len(exe_children) == len(initial_children) + 2
            assert exe.submit(os.getpid).result() != os.getpid()
            assert exe_children == get_children()
        assert initial_children == get_children()
    finally:
        cluster.stop_engines_sync()
        cluster.stop_controller_sync()
    assert initial_children == get_children()


@pytest.mark.slow
def test_client_provided_outside_ctx():
    import ipyparallel as ipp

    initial_children = get_children()
    cluster = ipp.Cluster(n=1, log_level=logging.FATAL)
    try:
        # It should not auto-start anything yet. For this part, we give it a
        # little time since it takes a while to create and connect to its
        # subprocesses.
        time.sleep(2)
        assert initial_children == get_children()
        # Once we synchronously start everything, then everything should proceed
        # as usual.
        client = cluster.start_and_connect_sync()
        client_children = get_children()
        assert client_children > initial_children
        assert len(client_children) == len(initial_children) + 2
        with ExecutorCtx("ipyparallel", client=client) as exe:
            assert client_children == get_children()
            assert exe.submit(os.getpid).result() != os.getpid()
            assert client_children == get_children()
        assert client_children == get_children()
    finally:
        cluster.stop_engines_sync()
        cluster.stop_controller_sync()
    assert initial_children == get_children()


@pytest.mark.slow
def test_client_provided_within_ctx():
    import ipyparallel as ipp

    initial_children = get_children()
    with ipp.Cluster(n=1, log_level=logging.FATAL) as client:
        client_children = get_children()
        assert client_children > initial_children
        assert len(client_children) == len(initial_children) + 2
        with ExecutorCtx("ipyparallel", client=client) as exe:
            assert client_children == get_children()
            assert exe.submit(os.getpid).result() != os.getpid()
            assert client_children == get_children()
        assert client_children == get_children()
    assert initial_children == get_children()


@pytest.mark.slow
def test_always_picklable_func():
    # If this crashes, everything will.
    with ExecutorCtx("ipyparallel", 1) as exe:
        exe.submit(os.getpid).result()


@pytest.mark.slow
def test_import_env_transferred_without_cloudpickle():
    # This test module sits outside the normal import hierarchy, so this will
    # crash unless the remote workers have replicated the import environment of
    # the main process.
    with ExecutorCtx("ipyparallel", 1) as exe:
        exe.submit(get_worker_id).result()


@pytest.mark.slow
def test_inner_func_pickling():
    # If this fails, it means that the pickler didn't figure out that the
    # function isn't at the global scope of its module.
    func = get_inner_func()

    with ExecutorCtx("ipyparallel", 1) as exe:
        assert exe.submit(func).result() == 123


@pytest.mark.slow
def test_lambda_pickling():
    # If this fails, it means the pickler can't handle lambdas properly.
    with ExecutorCtx("ipyparallel", 1) as exe:
        assert exe.submit(lambda: 123).result() == 123
        func = get_inner_func()
        assert exe.submit(lambda: func()).result() == 123
        assert exe.submit(lambda func=func: func()).result() == 123


@pytest.mark.slow
def test_double_picklability():
    # Here we verify that advanced pickling is available in both directions. We
    # send a lambda and get back a lambda.
    with ExecutorCtx("ipyparallel", 1) as exe:
        inner = lambda: 123  # NOQA
        outer = lambda: inner  # NOQA
        fut = exe.submit(outer)
        inner_ret = fut.result()
        ret = inner_ret()
        assert ret == 123
