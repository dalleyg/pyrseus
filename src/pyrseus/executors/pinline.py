"""
Provides a variant of `InlineExecutor` that tests the picklability of all
submitted tasks and their return values, using `pickle`.
"""

from functools import partial

from pyrseus.core.pickle import call_with_round_trip_pickling
from pyrseus.executors.inline import InlineExecutor


class PInlineExecutor(InlineExecutor):
    def __init__(self, **round_trip_kwargs):
        """
        Pickle-testing variant of `.InlineExecutor`, using the built-in `pickle`
        module.

        This variant pickles each submitted task and the task's results, using
        the built-in `pickle` module. This is primarily useful for
        troubleshooting pickling problems occurring in multi-process executors,
        by performing the pickling and unpickling locally.

        Here's an example showing a pickling failure cause by trying to pickle a
        lambda with an executor type that uses `pickle` instead of
        |cloudpickle|_.

        Consider this lambda.

            >>> import pickle
            >>> needs_cloudpickle = lambda: "this only works with cpnocatch, not pnocatch"

        It can't be pickled with `pickle`.

            >>> pickle.dumps(needs_cloudpickle, -1)
            Traceback (most recent call last):
            ...
            _pickle.PicklingError: Can't pickle ...

        Since this class tests pickling with `pickle`, it correctly fails, as it
        would in a multi-process executor that uses `pickle`.

            >>> with PInlineExecutor() as exe:
            ...     fut = exe.submit(needs_cloudpickle)
            >>> # As with InlineExecutor, the exception is only propagated out
            >>> # at result() time.
            >>> print(fut.result())
            Traceback (most recent call last):
            ...
            _pickle.PicklingError: Can't pickle ...

        Because the failure happens within the same process and thread as the
        submit call, it is easy to debug by tracing into it with a debugger.

        See :doc:`../plugins` for a list of related executors.

        :param round_trip_kwargs: keyword arguments passed to
            `call_with_round_trip_pickling` for each submitted task.
        """
        super().__init__()
        self._round_trip_kwargs = round_trip_kwargs

    def submit(self, fcn, /, *args, **kwargs):
        testing_closure = partial(
            call_with_round_trip_pickling,
            fcn,
            args,
            kwargs,
            **self._round_trip_kwargs,
        )
        return super().submit(testing_closure)
