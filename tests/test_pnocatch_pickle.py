from unittest import TestCase

from pyrseus.executors.pnocatch import PNoCatchExecutor

Cls = PNoCatchExecutor


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
            with self.assertRaisesRegex(Exception, "Can't pickle"):
                exe.submit(lambd)

    def test_return_lambda(self):
        with Cls() as exe:
            with self.assertRaisesRegex(Exception, "Can't pickle"):
                exe.submit(make_lambda)

    def test_double_unpicklable(self):
        with Cls() as exe:
            func = make_double_unpicklable()
            with self.assertRaisesRegex(Exception, "Can't pickle"):
                exe.submit(func)
