"""
Provides a simple serial executor that does *not* capture exceptions in
submitted futures. This is primarily useful for troubleshooting when one wants
to enter a debugger as early and as easily as possible.
"""

from concurrent.futures import Executor, Future
from typing import Callable, TypeVar

__all__ = ["NoCatchExecutor"]

# Represents the generic return type of a submitted callable.
Ret = TypeVar("Ret")


class NoCatchExecutor(Executor):
    def __init__(self):
        """
        Creates a simple serial `~concurrent.futures.Executor` that evaluates
        tasks immediately upon submission, and does *not* capture task
        exceptions in their futures.

        This is primarily useful for troubleshooting when one wants to enter a
        debugger as early and as easily as possible, at the cost of non-standard
        error handling.

        Consider the following function that raises an exception:

            >>> def raises():
            ...     raise RuntimeError("An exception was raised by our function.")

        With this class, the exception is propagated out immediately at
        `~NoCatchExecutor.submit` time.

            >>> with NoCatchExecutor() as exe:
            ...     exe.submit(raises)  # <--- NOTE: no fut.result() needed
            Traceback (most recent call last):
            ...
            RuntimeError: An exception was raised by our function.

        See :doc:`../plugins` for a list of related executors.
        """
        self._closing = False

    def submit(self, fcn: Callable[..., Ret], /, *args, **kwargs) -> Future[Ret]:
        """
        Immediately evaluates ``fcn(*args, **kwargs)`` and embeds the result in
        a `~concurrent.futures.Future`. Unlike with standard executors, this
        method does *not* capture exceptions. They are propagated out
        immediately.
        """
        # This implementation is the same as InlineExecutor.submit, except this
        # one does not have a try-except-else construct. This breaks the normal
        # `~concurrent.futures.Executor` protocol, but it can make debugging
        # easier if users want exceptions to flow through to the default handler
        # instead of being captured by the returned
        # `~concurrent.futures.Future`.

        # To be consistent with `~concurrent.futures.ProcessPoolExecutor`,
        # disallow new submissions once shutdown has started.
        if self._closing:
            raise RuntimeError(
                "Submissions are not allowed to an executor that is shutting down."
            )

        # Eagerly execute the function *without* a try-except guard. Users who
        # want the standard guard should use `.InlineExecutor` instead of
        # `.NoCatchExecutor`.
        result = fcn(*args, **kwargs)

        # Handle the result. In this class we never call
        # `fut.set_exception(exc)` because we propagate exceptions back to the
        # user instead of capturing them.
        fut: Future[Ret] = Future()
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
