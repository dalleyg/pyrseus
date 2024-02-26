"""
Tests that `call_with_round_trip_pickling` and `try_pickle_round_trip` are able
to detect common picklability problems with exotic function types when they're
used in multi-process settings.
"""

import os
import pickle
import sys
from contextlib import contextmanager
from textwrap import dedent
from unittest import TestCase

import pytest

from pyrseus.core.pickle import call_with_round_trip_pickling, try_pickle_round_trip


def get_mul_2():
    def mul_2(x):
        return 2 * x

    return mul_2


def get_lambda():
    return lambda: 42


@contextmanager
def MainFuncCtx():
    main_mod = sys.modules["__main__"]
    main_globals = main_mod.__dict__
    name = "pyrseus_test_pickle_main_func"
    assert name not in main_globals  # check for improper cleanup of other tests
    exec(f"def {name}(): return 42", main_globals)
    yield main_globals[name]
    del main_globals[name]  # clean up after ourselves


@contextmanager
def MainClsCtx():
    main_mod = sys.modules["__main__"]
    main_globals = main_mod.__dict__
    name = "PyrseusTestPickleMainCls"
    assert name not in main_globals  # check for improper cleanup of other tests
    src = dedent(
        f"""
        class {name}:
            def mthd(self):
                return 42
        """
    ).strip()
    exec(src, main_globals)
    yield main_globals[name]
    del main_globals[name]  # clean up after ourselves


class TestTryPickleRoundTrip(TestCase):
    """
    Tests the basic functionality of `try_pickle_round_trip` with various tricky
    objects, including its ``hide_main`` behavior.
    """

    def test_normal_func(self):
        # Normal, importable functions work fine.
        assert try_pickle_round_trip(os.getpid) is os.getpid

    def test_main_func(self):
        with MainFuncCtx() as main_func:
            # We catch this case of a __main__ function being typically not
            # transferrable across processes.
            with pytest.raises(RuntimeError, match="Blocked attempt"):
                try_pickle_round_trip(main_func)

    def test_main_func_no_hide(self):
        with MainFuncCtx() as main_func:
            # The default picklers will happily pickle up a function defined in
            # __main__, since it doesn't know that the use may want to unpickle
            # it in a new process.
            assert try_pickle_round_trip(main_func, hide_main=False)() == 42

    def test_inner_func(self):
        # The built-in pickler already does a good job detecting functions that
        # are not in their owning module's globals dict. We report the native
        # exception.
        with pytest.raises(pickle.PicklingError):
            try_pickle_round_trip(get_mul_2())

    def test_lambda(self):
        # Similarly, the existing handling of lambdas is fine (given that pickle
        # can't handle them).
        with pytest.raises(pickle.PicklingError):
            try_pickle_round_trip(lambda: 42)

    def test_get_lambda(self):
        # But try_pickle_round_trip doesn't catch problems with unpicklable
        # return values. See TestCallWithRoundTripPickling for how
        # call_with_round_trip_pickling helps with that use case.
        assert try_pickle_round_trip(get_lambda)()() == 42

    def test_inst_method_of_normal_class(self):
        # Normal bound methods of importable classes work fine.
        data = {"a": 42}
        assert try_pickle_round_trip(data.get)("a") == 42

    def test_inst_method_of_main_class(self):
        with MainClsCtx() as MainCls:
            # We catch this case of a bound instance method for a class that was
            # defined in __main__.
            instance = MainCls()
            with pytest.raises(RuntimeError, match="Blocked attempt"):
                try_pickle_round_trip(instance.mthd)

    def test_inst_method_of_main_class_no_hide(self):
        with MainClsCtx() as MainCls:
            # The default picklers will happily pickle up a bound instance
            # method for a class that was defined in __main__, since it doesn't
            # know that the use may want to unpickle it in a new process.
            instance = MainCls()
            assert try_pickle_round_trip(instance.mthd, hide_main=False)() == 42


class TestCallWithRoundTripPickling(TestCase):
    """
    Tests the basic functionality of `call_with_round_trip_pickling`.
    This is very similar to `TestTryPickleRoundTrip`.
    """

    def test_normal_func(self):
        closure_args = (os.getpid, (), {})
        ret = call_with_round_trip_pickling(*closure_args)
        assert ret == os.getpid()

    def test_main_func(self):
        with MainFuncCtx() as main_func:
            closure_args = (main_func, (), {})
            with pytest.raises(RuntimeError, match="Blocked attempt"):
                call_with_round_trip_pickling(*closure_args)

    def test_main_func_no_hide(self):
        with MainFuncCtx() as main_func:
            closure_args = (main_func, (), {})
            ret = call_with_round_trip_pickling(*closure_args, hide_main=False)
            assert ret == 42

    def test_inner_func(self):
        closure_args = (get_mul_2, (), {})
        with pytest.raises(pickle.PicklingError):
            call_with_round_trip_pickling(*closure_args)

    def test_lambda(self):
        closure_args = (lambda: 42, (), {})
        with pytest.raises(pickle.PicklingError):
            call_with_round_trip_pickling(*closure_args)

    def test_get_lambda(self):
        # Here we can detect the picklability problem of get_lambda's return
        # value since we actually call the function and check the picklability
        # of the result.
        closure_args = (get_lambda, (), {})
        with pytest.raises(pickle.PicklingError):
            call_with_round_trip_pickling(*closure_args)

    def test_inst_method_of_normal_class(self):
        data = {"a": 42}
        closure_args = (data.get, ("a",), {})
        assert call_with_round_trip_pickling(*closure_args) == 42

    def test_inst_method_of_main_class(self):
        with MainClsCtx() as MainCls:
            instance = MainCls()
            closure_args = (instance.mthd, (), {})
            with pytest.raises(RuntimeError, match="Blocked attempt"):
                call_with_round_trip_pickling(*closure_args)

    def test_inst_method_of_main_class_no_hide(self):
        with MainClsCtx() as MainCls:
            instance = MainCls()
            closure_args = (instance.mthd, (), {})
            ret = call_with_round_trip_pickling(*closure_args, hide_main=False)
            assert ret == 42
