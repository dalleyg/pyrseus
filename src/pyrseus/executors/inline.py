"""
Provides a simple serial executor that captures exceptions in the standard way.

This is useful:

- for running executor-based code serially without having to rewrite all of the
  control flow, and
- making it easy to use a debugger to trace into task functions.
"""

from concurrent.futures import Executor, Future
from typing import Callable, TypeVar

__all__ = ["InlineExecutor"]

# Represents the generic return type of a submitted callable.
Ret = TypeVar("Ret")


class InlineExecutor(Executor):
    def __init__(self):
        """
        Creates an `~concurrent.futures.Executor` that evaluates the task
        immediately upon submission, trapping exceptions like normal executors
        do.

        As with the only built-in within-process executor,
        `~concurrent.futures.ThreadPoolExecutor`, arbitrary callables can be
        used, including non-picklable ones like lambdas:

            >>> import os
            >>> with InlineExecutor() as exe:
            ...     non_picklable = lambda: os.getpid()   # can't be pickled
            ...     fut = exe.submit(non_picklable)       # but works anyway
            ...     worker_pid_is_my_pid = fut.result()   # no pickling error
            ...     assert os.getpid() == worker_pid_is_my_pid

        See :doc:`../plugins` for a list of related executors.
        """
        self._closing = False

    def submit(self, fcn: Callable[..., Ret], /, *args, **kwargs) -> Future[Ret]:
        """
        Immediately evaluates ``fcn(*args, **kwargs)`` and embeds the result in
        a `~concurrent.futures.Future`. As with standard executors, any
        exceptions that occur are caught and recorded in that returned
        `~concurrent.futures.Future`.
        """
        # This implementation is the same as NoCatchExecutor.submit, except this
        # one captures all exceptions in the returned
        # `~concurrent.futures.Future` object, like the standard
        # `~concurrent.futures` executors do.

        # To be consistent with `~concurrent.futures.ProcessPoolExecutor`,
        # disallow new submissions once shutdown has started.
        if self._closing:
            raise RuntimeError(
                "Submissions are not allowed to an executor that is shutting down."
            )

        fut: Future[Ret] = Future()
        try:
            # Eagerly execute the function. This makes it easy for users to use
            # `pdb.set_trace` and similar troubleshooting techniques to debug
            # `fcn`.
            result = fcn(*args, **kwargs)

        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)

        return fut

    def shutdown(self, *args, **kwargs):
        """
        As a minor improvement on the base class' method, this override
        disallows submissions after a shutdown has been started. This can assist
        with finding bugs in user code.
        """
        self._closing = True
        super().shutdown(*args, **kwargs)
