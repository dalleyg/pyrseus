"""
Provides a variant of `NoCatchExecutor` that tests the picklability of all
submitted tasks and their return values, using |cloudpickle|_.
"""

from functools import partial

import cloudpickle

from pyrseus.core.pickle import call_with_round_trip_pickling
from pyrseus.executors.nocatch import NoCatchExecutor


class CpNoCatchExecutor(NoCatchExecutor):
    """
    Pickle-testing variant of `.NoCatchExecutor`, using the 3rd party
    |cloudpickle|_ module.

    This variant pickles each submitted task and the task's results, using the
    built-in |cloudpickle|_ module that understands things like lambdas. This is
    primarily useful for troubleshooting pickling problems occurring in
    multi-process executors, by performing the pickling and unpickling locally.

    Consider this lambda.

        >>> import pickle
        >>> needs_cloudpickle = lambda: "this works if using cloudpickle for pickling"

    It can't be pickled with `pickle`.

        >>> pickle.dumps(needs_cloudpickle, -1)
        Traceback (most recent call last):
        ...
        _pickle.PicklingError: Can't pickle ...

    But it can be pickled with |cloudpickle|_.

        >>> print(pickle.loads(cloudpickle.dumps(needs_cloudpickle, -1))())
        this works if using cloudpickle for pickling

    Since this class uses |cloudpickle|_ for pickling, it works fine.

        >>> with CpNoCatchExecutor() as exe:
        ...     fut = exe.submit(needs_cloudpickle)
        ...     print(fut.result())
        this works if using cloudpickle for pickling

    See :doc:`../plugins` for a list of related executors.
    """

    def submit(self, fcn, /, *args, **kwargs):
        testing_closure = partial(
            call_with_round_trip_pickling,
            fcn,
            args,
            kwargs,
            dumps=cloudpickle.dumps,
            loads=cloudpickle.loads,
            # __main__ dependency problems are very rare with cloudpickle, so
            # for now we disable the temporary sys.modules patching.
            hide_main=False,
        )
        return super().submit(testing_closure)
