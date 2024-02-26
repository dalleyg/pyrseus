from unittest import TestCase

from pyrseus.executors.cpnocatch import CpNoCatchExecutor

Cls = CpNoCatchExecutor


def picklable():
    return 1


def make_lambda():
    return lambda: 2


def make_double_unpicklable():
    return lambda: lambda: 3


class TestPickling(TestCase):
    def test_picklable(self):
        with Cls() as exe:
            fut = exe.submit(picklable)
            ret = fut.result()
            assert ret == 1

    def test_lambda(self):
        with Cls() as exe:
            lambd = make_lambda()
            fut = exe.submit(lambd)
            ret = fut.result()
            assert ret == 2

    def test_return_lambda(self):
        with Cls() as exe:
            fut = exe.submit(make_lambda)
            ret_func = fut.result()
            ret = ret_func()
            assert ret == 2

    def test_double_unpicklable(self):
        with Cls() as exe:
            func = make_double_unpicklable()
            fut = exe.submit(func)
            ret_func = fut.result()
            ret = ret_func()
            assert ret == 3
