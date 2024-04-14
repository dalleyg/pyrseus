"""
Provides a wrapper around `~pyrseus.ctx.mgr.ExecutorCtx` to make it useful in
interactive use cases.
"""
import sys

from pyrseus.ctx.mgr import ExecutorCtx


class _CtxMgrToRAIIProxy:
    """
    Wrapper proxy for a context manager that turns it into an `RAII
    <https://en.cppreference.com/w/cpp/language/raii>`_-like class.

    Background:

     - Python context managers are designed to be used in ``with`` statements,
       typically deferring heavy work till their ``__enter__`` method is called.
       When control flow leaves the ``with`` statement, the context manager's
       ``__exit__`` method is called, resulting in eager and reliable cleanup.

     - In contrast, C++ RAII classes perform the heavy initialization in their
       constructors and all of the shutdown in their destructors. This works
       well in C++ because (a) C++ uses block scoping instead of function
       scoping of variables, (b) it maintains a strong distinction between
       stack-based objects and heap-based ones, and (c) it has strong guarantees
       about how exceptions, constructors, and destructors interact. This makes
       it the the correct way to ensure eager and reliable cleanup in C++.

    What this proxy class does is to trigger the ``__enter__`` method in its
    constructor, proxy all public member access to the object returned by
    ``__enter__``, and then call ``__exit__`` at finalization time.

    See `.get_executor` for when to use this vs. not use it.
    """

    def __init__(self, ctx: ExecutorCtx):
        self._ctx = ctx
        self._obj = ctx.__enter__()

    def __del__(self):
        del self._obj
        self._ctx.__exit__(*sys.exc_info())

    def __enter__(self):
        raise RuntimeError(
            "This proxy should not be used as a context manager. "
            "Directly use the original context manager instead."
        )

    def __exit__(self, *exc_info):
        raise RuntimeError(
            "This proxy should not be used as a context manager. "
            "Directly use the original context manager instead."
        )

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(f"Only public members are proxied, not: {name}")
        return getattr(self._obj, name)


def get_executor(*args, **kwargs):
    """
    FOR INTERACTIVE USE ONLY: This function is like `.ExecutorCtx`, but
     - this function pre-calls the executor after ``__enter__`` method and
     - attempts to perform its ``__exit__`` behavior in a ``__del__`` finalizer.

    This can be useful for interactive use cases where the worker pool should
    stay alive till the whole process exits.

    All of this function's arguments are passed to `.ExecutorCtx`.

    Here is an example of the same executor being used across multiple blocks of
    code. We explicitly delete the executor at the end to encourage eager
    cleanup of our worker pool.

        >>> exe = get_executor("thread", 1)
        >>> futs = [exe.submit(lambda x: x, x) for x in range(10)]
        >>> print(sum(fut.result() for fut in futs))
        45

        >>> futs = [exe.submit(lambda x: x*x, x) for x in range(10)]
        >>> print(sum(fut.result() for fut in futs))
        285

        >>> del exe  # try to eagerly cleanup

    .. note::

        If you have an interactive use case like an IPython session or a Jupyter
        notebook where:

        - you will be submitting parallel work in multiple different cells,
        - your worker pool startup time is non-trivial and/or you have complex
          parameters such as an initializer function, *and*
        - you don't mind the worker pool staying alive and consuming resources
          until the interactive process fully exits, then

        this function may be right for you. But if even one of those statements
        is false, consider using the more robust `.ExecutorCtx` instead in a
        proper ``with`` block.

        See the |nb_get_executor|_ notebook for additional guidance about the
        tradeoffs between `.ExecutorCtx` and `.get_executor`.

    """
    return _CtxMgrToRAIIProxy(ExecutorCtx(*args, **kwargs))
