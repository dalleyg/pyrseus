"""
Provides a drop-in replacement for the built-in `.ProcessPoolExecutor` that uses
|cloudpickle|_ for pickling tasks and their return values, instead of `pickle`.
"""

from concurrent.futures import ProcessPoolExecutor

import cloudpickle
from pyrseus.core.pickle import CustomPickledClosure


class CpProcessPoolExecutor(ProcessPoolExecutor):
    """
    A drop-in replacement for the built-in `.ProcessPoolExecutor` that uses
    |cloudpickle|_ for pickling tasks and their return values, instead of
    `pickle`.

    Consider this lambda.

        >>> needs_cloudpickle = lambda: 123

    It can't be pickled with `pickle`.

        >>> import pickle
        >>> pickle.dumps(needs_cloudpickle, -1)
        Traceback (most recent call last):
        ...
        _pickle.PicklingError: Can't pickle ...

    And since the built-in `.ProcessPoolExecutor` uses `pickle`, it can't handle
    it:

        >>> with ProcessPoolExecutor(1) as exe:
        ...     fut = exe.submit(needs_cloudpickle)
        ...     print(fut.result())
        Traceback (most recent call last):
        ...
        _pickle.PicklingError: Can't pickle ...

    But `.CpProcessPoolExecutor` is a thin wrapper around `.ProcessPoolExecutor`
    that uses |cloudpickle|_ for serialization, so it works fine.

        >>> with CpProcessPoolExecutor(1) as exe:
        ...     fut = exe.submit(needs_cloudpickle)
        ...     print(fut.result())
        123

    See :doc:`../plugins` for a list of related executors.
    """

    def submit(self, fcn, /, *args, **kwargs):
        closure = CustomPickledClosure(
            fcn,
            args,
            kwargs,
            dumps=cloudpickle.dumps,
            loads=cloudpickle.loads,
        )
        return super().submit(closure)
