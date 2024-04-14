"""
Helpers for writing `~concurrent.futures.Executor` classes and
`~pyrseus.ctx.mgr.ExecutorCtx` plugins that need to interact with `pickle`-based
serialization.

There are two main use cases:

- To change the serializer to a more powerful one like |cloudpickle|_. For
  examples, see the source code of:

  - `pyrseus.executors.cpprocess`: wrapper for
    `~concurrent.futures.ProcessPoolExecutor` that uses |cloudpickle|_ instead
    of `pickle` for task and result serialization.

  - `pyrseus.ctx.plugins.cpmpi4py`: wrapper for
    `~mpi4py.futures.MPIPoolExecutor` that uses |cloudpickle|_ instead of
    `pickle` for task and result serialization.

- To help users troubleshoot serialization problems. For examples, see the
  source code of:

  - `pyrseus.executors.pinline`: serializes tasks and results in the same thread
    and process as the ``submit`` call, using `pickle`.

  - `pyrseus.executors.cpinline`:  serializes tasks and results in the same
    thread and process as the ``submit`` call, using |cloudpickle|_.
"""

import inspect
import pickle
import sys
from functools import cache
from threading import Lock
from typing import Any, Callable, Dict, Tuple, TypeVar

Ret = TypeVar("Ret")
"""
Represents the generic return type of a submitted callable.
"""


class _HideMain:
    """
    Helper for `.try_pickle_round_trip` that temporarily replaces
    ``sys.modules['__main__']``, causing any attribute access to raise an
    exception. This can make troubleshooting pickling problems easier.
    """

    def __init__(self, hide_from):
        self._hide_from = hide_from

    def __getattr__(self, name):
        raise RuntimeError(
            f"Blocked attempt to read {name} from __main__ while pickling "
            f"{self._hide_from}."
        )


_hide_main_lock = Lock()
"""
Protects against concurrent calls to `.try_pickle_round_trip` from permanently
corrupting ``sys.modules``.
"""


def try_pickle_round_trip(
    obj, *, dumps=pickle._dumps, protocol=-1, loads=pickle._loads, hide_main=True
):
    """
    Attempts to pickle and unpickle ``obj`` using ``dumps`` and ``loads``, to
    help with testing and/or troubleshooting.

    Example
    -------

    Here's an example of an object that can be pickled and unpickled without
    trouble:

        >>> try_pickle_round_trip(42)
        42

    Here's one that acts like it's picklable and unpicklable:

        >>> # simulate defining a trivial function in __main__, as happens
        >>> # for scripts and notebooks.
        >>> exec(
        ...     'def _func_in_main_for_try_pickle_round_trip(): return 42',
        ...     sys.modules['__main__'].__dict__,
        ... )
        >>> # retrieve it
        >>> func = sys.modules['__main__']._func_in_main_for_try_pickle_round_trip

        >>> # technically, it's picklable and unpicklable, as long as the
        >>> # unpickling happens in the same process:
        >>> pickle.loads(pickle.dumps(func, -1))()
        42

    But if the unpickling happens in another process that doesn't replicate the
    original's ``__main__`` module, then unpickling will fail, often with a
    confusing error message.

    But with this function, we can simulate those unpickling problems, but at
    pickling time when they're easier to troubleshoot:

        >>> # Here it fails with a slightly better error message, and by
        >>> # default using the pure Python pickler that's easier to debug
        >>> # than the C one.
        >>> try_pickle_round_trip(func)
        Traceback (most recent call last):
        ...
          File ...pickle.py... in save_global
        ...
        RuntimeError: Blocked attempt to read
        _func_in_main_for_try_pickle_round_trip from __main__ while pickling
        <function _func_in_main_for_try_pickle_round_trip at ...>.

        >>> # Clean up
        >>> del sys.modules['__main__']._func_in_main_for_try_pickle_round_trip

    .. warning::

       When ``hide_main=True``, this function mutates global state by
       temporarily replacing ``sys.modules['__main__']``. This function is
       threadsafe with respect to itself, but if some other concurrent code
       requires access to the ``__main__`` module *via* ``sys.modules``, then
       they may see unexpected exceptions. Such situations are rare.

    :param obj: the object whose picklability is being tested

    :param protocol: pickle protocol number to use. This is passed to ``dumps``
        as a keyword argument.

    :param dumps: a function that serializes objects to a pickle bytestring.
        This defaults to the pure Python `pickle._dumps` function. It is slower
        than the C `pickle.dumps` function, but it's easier to troubleshoot with
        a Python debugger.

    :param loads: a function that loads objects from a pickle bytestring. This
        defaults to the pure Python `pickle._loads` function. It is slower than
        the C `pickle.loads` function, but it's easier to troubleshoot with a
        Python debugger.

    :param hide_main: whether to hide the ``__main__`` module during the pickle
        operation. This is done by temporarily replacing
        ``sys.modules['__main__']`` with a proxy object that raises an exception
        on any attribute access. This makes it much easier to detect and
        troubleshoot pickling problems that arise from objects that belong to a
        script or notebook instead of a Python module, without needing to
        perform the unpickling in a separate process where it's harder to debug.
    """
    if hide_main:
        with _hide_main_lock:
            old_main = sys.modules["__main__"]
            try:
                sys.modules["__main__"] = _HideMain(obj)
                pickled = dumps(obj, protocol=protocol)
                reconstructed = loads(pickled)
            finally:
                sys.modules["__main__"] = old_main
    else:
        pickled = dumps(obj, protocol=protocol)
        reconstructed = loads(pickled)
    return reconstructed


@cache
def get_round_trip_keywords():
    """
    Helper for creating plugins that wrap `try_pickle_round_trip` and/or
    `call_with_round_trip_pickling`.

    >>> sorted(get_round_trip_keywords())
    ['dumps', 'hide_main', 'loads', 'protocol']
    """
    return {
        p.name
        for p in inspect.signature(try_pickle_round_trip).parameters.values()
        if p.kind is inspect.Parameter.KEYWORD_ONLY
    }


def call_with_round_trip_pickling(func, args, kwargs, **round_trip_kwargs):
    """
    Calls ``func(*args, **kwargs)``, but (a) running those three things through
    `try_pickle_round_trip` first, and then (b) running the result through
    `try_pickle_round_trip` too.

    This function is designed for serial executors that help detect picklability
    problems in submitted tasks.

    :param func: function to call after round-trip pickling it

    :param args: positional arguments to pass to ``func`` after round trip
        pickling

    :param kwargs: keyword arguments to pass to ``func`` after round trip
        pickling

    :param round_trip_kwargs: passed to `try_pickle_round_trip` to override the
        round trip pickling settings

    :return: the return value of ``func(*args, **kwargs)``, after doing the two
        round trip pickling tests
    """
    r_func, r_args, r_kwargs = try_pickle_round_trip(
        (func, args, kwargs), **round_trip_kwargs
    )
    orig_ret = r_func(*r_args, **r_kwargs)
    ret = try_pickle_round_trip(orig_ret, **round_trip_kwargs)
    return ret


class OncePickledObject:
    """
    Creates a wrapper around an arbitrary Python object that forces it to be
    pickled with the chosen pickler, but a copy of the original unwrapped object
    is returned at unpickling time.

    This wrapper does not provide any proxying of attributes or methods. It is
    only intended to be used when users know the object will be pickled and
    unpickled exactly once before use.

    >>> opo123 = OncePickledObject(123, pickle.dumps, pickle.loads)
    >>> opo123
    <pyrseus.core.pickle.OncePickledObject object at ...>
    >>> pickle.loads(pickle.dumps(opo123, -1))
    123
    """

    def __init__(self, obj, dumps, loads):
        self._obj = obj
        self._dumps = dumps
        self._loads = loads

    def __reduce_ex__(self, protocol: int):
        # Pickle the object we're wrapping, using the chosen pickler, returning
        # a bytestring.
        pickled = self._dumps(self._obj, protocol=protocol)
        # Tell the pickler that's calling us that it can reconstruct the
        # original unwrapped object by calling the chosen unpickler on the
        # bytestring created from the original object we wrapped.
        return self._loads, (pickled,)


class CustomPickledClosure:
    def __init__(
        self,
        func: Callable,
        args: Tuple,
        kwargs: Dict,
        dumps: Callable[..., bytes],
        loads: Callable[[bytes], Any],
    ):
        """
        Wraps a function and its arguments into a nullary closure that forces a
        chosen pickler to be used for the closure and its return value.

        - The closed function will remain wrapped by this class when it
          undergoes round trip pickling.

        - The return value of the closed function will be wrapped in a
          `.OncePickledObject` that uses the same ``dumps`` and ``loads``
          functions as the wrapper.

        This is useful for wrapping functions that will be submitted to
        executors like `~concurrent.futures.ProcessPoolExecutor` with workers in
        other processes. This forces the chosen pickler to be used for
        serialization, instead of whatever default pickler the executor normally
        uses.

        :param func: the function whose pickling behavior we're overriding

        :param args: the positional arguments to ``func`` that we're closing
            over

        :param kwargs: the keyword arguments to ``func`` that we're closing over

        :param dumps: a `pickle.dumps`-like function that will be used for
            serializing the closure contents when this wrapper is pickled. This
            function must accept a ``protocol`` keyword argument.

        :param loads: a `pickle.loads`-like function that will be used for
            deserializing the closure contents when the pickled form of this
            wrapper is unpickled. To avoid bootstrapping problems, this function
            must itself be picklable by the built-in `pickle.dumps`.
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._dumps = dumps
        self._loads = loads

    def __reduce_ex__(self, protocol: int):
        # First, we need to pickle up the unpickler. To avoid bootstrapping
        # problems if the unpickling happens in another process, this is done
        # with pickle.dumps itself.
        pickled_loads = pickle.dumps(self._loads, protocol=protocol)
        # Now pickle up the whole closure, using the requested serializer. For
        # efficiency, we don't re-serialize self._loads
        args = [self._func, self._args, self._kwargs, self._dumps]
        dumped_args = self._dumps(args, protocol=protocol)
        # This instructs the unpickler to use our special factory that undoes
        # the extra nested pickling.
        return type(self)._hydrate, (pickled_loads, dumped_args)

    @classmethod
    def _hydrate(cls, pickled_loads: bytes, dumped_args: bytes):
        loads = pickle.loads(pickled_loads)
        hydrated_args = loads(dumped_args)
        hydrated_args.append(loads)
        return cls(*hydrated_args)

    def __call__(self):
        # Call the underlying (reconstructed) function.
        raw_ret = self._func(*self._args, *self._kwargs)
        # Return a wrapper object that will produce a copy of the underlying
        # object when pickled and unpickled.
        return OncePickledObject(raw_ret, self._dumps, self._loads)
