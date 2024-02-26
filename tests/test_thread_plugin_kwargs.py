import inspect
import os
import sys
import threading
from concurrent.futures import ProcessPoolExecutor
from unittest import TestCase
from unittest.mock import Mock

from pyrseus.ctx.mgr import ExecutorCtx

PLUGIN_NAME = "thread"


def get_worker_id():
    return threading.get_ident()


def set_tls_worker_id(tls):
    tls.worker_id = get_worker_id()


def get_tls_worker_id(tls):
    return tls.worker_id


class TestThreadPoolKwargs(TestCase):
    POOL_SIG = inspect.signature(ProcessPoolExecutor)

    def test_ignored(self):
        """
        Ensure that various the parameters that `.ExecutorCtx` is supposed to
        ignore for this executor type are in fact ignored.
        """
        # For all parameters that are supposed to be ignored, pass in a poisoned
        # argument: Mock() is sufficiently incompatible for us.
        mock = Mock()
        kw = {"mp_context": mock}
        if sys.version_info >= (3, 11):
            kw["max_tasks_per_child"] = mock
        with ExecutorCtx(PLUGIN_NAME, 1, **kw) as exe:
            assert exe.submit(os.getpid).result() == os.getpid()
            assert exe.submit(get_worker_id).result() != get_worker_id()
        assert not mock.called
        assert len(mock.method_calls) == 0
        assert len(mock.mock_calls) == 0

    def test_bad_kwarg_not_ignored(self):
        with self.assertRaises(TypeError):
            with ExecutorCtx(PLUGIN_NAME, this_is_not_a_valid_kwarg_name=123):
                pass

    def test_initializer(self):
        tls = threading.local()
        # First verify that tls.worker_id isn't set in the main thread.
        with self.assertRaises(AttributeError):
            tls.worker_id
        # If we don't run the initializer, then the code fails in threads.
        with ExecutorCtx(PLUGIN_NAME, 1) as exe:
            fut = exe.submit(get_tls_worker_id, tls)
            with self.assertRaises(AttributeError):
                fut.result()
        # But we can initialize thread local storage, and then it works fine.
        with ExecutorCtx(
            PLUGIN_NAME, 1, initializer=set_tls_worker_id, initargs=(tls,)
        ) as exe:
            assert exe.submit(get_tls_worker_id, tls).result() != get_worker_id()

    def test_thread_name_prefix(self):
        PREFIX = "TestThread-PYRSEUS-CTX-TTPK-TTNP"

        def count_threads_with_prefix():
            count = 0
            for thread in threading.enumerate():
                if thread.name.startswith(PREFIX):
                    count += 1
            return count

        assert count_threads_with_prefix() == 0
        with ExecutorCtx(PLUGIN_NAME, 1, thread_name_prefix=PREFIX) as exe:
            fut = exe.submit(get_worker_id)
            assert count_threads_with_prefix() == 1
            assert fut.result() != get_worker_id()
            assert count_threads_with_prefix() == 1
        assert count_threads_with_prefix() == 0
