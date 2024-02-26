"""
Tests that overriding ``dumps`` and ``loads`` in the `pyrseus.core.pickle`
classes and functions works as expected.
"""

import pickle
from unittest import TestCase

from pyrseus.core.pickle import (
    CustomPickledClosure,
    OncePickledObject,
    call_with_round_trip_pickling,
    try_pickle_round_trip,
)

LOG = []


def logged_dumps(obj, protocol=-1):
    pickled = pickle.dumps(obj, protocol=protocol)
    LOG.append("pickled")
    return pickled


def logged_loads(obj):
    ret = pickle.loads(obj)
    LOG.append("unpickled")
    return ret


class LogResettingMixin:
    def setUp(self):
        LOG[:] = []

    def tearDown(self):
        LOG[:] = []


def add_10(x):
    return x + 10


class TestTroubleshootingFuncs(LogResettingMixin, TestCase):
    """
    Verifies that `try_pickle_round_trip` and `call_with_round_trip_pickling`
    honor their ``dumps`` and ``loads`` arguments.
    """

    def test_try_pickle_round_trip(self):
        assert LOG == []
        ret = try_pickle_round_trip(
            add_10,
            dumps=logged_dumps,
            loads=logged_loads,
        )
        assert ret is add_10
        assert LOG == ["pickled", "unpickled"]

    def test_call_with_round_trip_pickling(self):
        assert LOG == []
        closure_args = (add_10, (1,), {})
        ret = call_with_round_trip_pickling(
            *closure_args,
            dumps=logged_dumps,
            loads=logged_loads,
        )
        assert ret == 11
        assert LOG == ["pickled", "unpickled", "pickled", "unpickled"]


class TestOncePickledObject(LogResettingMixin, TestCase):
    """
    Verifies that `OncePickledObject` interacts with picklers in the expected
    way.
    """

    def test_trigger_timing(self):
        assert LOG == []

        opo = OncePickledObject(42, logged_dumps, logged_loads)
        assert LOG == []

        pickled = pickle.dumps(opo, -1)
        assert LOG == ["pickled"]

        reconstructed = pickle.loads(pickled)
        assert LOG == ["pickled", "unpickled"]
        assert isinstance(reconstructed, int)  # not OncePickledObject
        assert reconstructed == 42


class TestCustomPickledClosure(LogResettingMixin, TestCase):
    """
    Verifies that `CustomPickledClosure` interacts with picklers in the expected
    way.
    """

    def test_trigger_timing(self):
        assert LOG == []

        closure = CustomPickledClosure(
            add_10,
            (2,),
            {},
            logged_dumps,
            logged_loads,
        )
        assert LOG == []

        pickled_closure = pickle.dumps(closure, -1)
        assert LOG == ["pickled"]

        reconstructed_closure = pickle.loads(pickled_closure)
        assert LOG == ["pickled", "unpickled"]
        assert callable(reconstructed_closure)

        wrapped_ret = reconstructed_closure()
        assert LOG == ["pickled", "unpickled"]
        assert isinstance(wrapped_ret, OncePickledObject)  # not int

        pickled_ret = pickle.dumps(wrapped_ret, -1)
        assert LOG == ["pickled", "unpickled", "pickled"]

        unwrapped_ret = pickle.loads(pickled_ret)
        assert LOG == ["pickled", "unpickled", "pickled", "unpickled"]
        assert unwrapped_ret == 12
