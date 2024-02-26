from unittest import TestCase

import pytest
from pyrseus.executors.pinline import PInlineExecutor

Cls = PInlineExecutor


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
            with pytest.raises(Exception, match="Can't pickle"):
                fut.result()

    def test_return_lambda(self):
        with Cls() as exe:
            fut = exe.submit(make_lambda)
            with pytest.raises(Exception, match="Can't pickle"):
                fut.result()

    def test_double_unpicklable(self):
        with Cls() as exe:
            func = make_double_unpicklable()
            fut = exe.submit(func)
            with pytest.raises(Exception, match="Can't pickle"):
                fut.result()
