"""
Provides a variant of `InlineExecutor` that tests the picklability of all
submitted tasks and their return values, using |cloudpickle|_.
"""

from functools import partial

import cloudpickle

from pyrseus.core.pickle import call_with_round_trip_pickling
from pyrseus.executors.inline import InlineExecutor


class CpInlineExecutor(InlineExecutor):
    """
    Pickle-testing variant of `.InlineExecutor`, using the 3rd party
    |cloudpickle|_ module.

    Summary
    -------

    - *Common Use Cases:* serially troubleshooting pickling problems for
      multi-process executors that use |cloudpickle|_ for serialization.

    - *Concurrency:* This is a non-concurrent, serial-only executor. All tasks
      are immediately run in the same process and thread they were submitted in.

    - *Exceptions:* This plugin has standard exception-handling semantics: all
      task-related exceptions are captured in the task's future.

    - *3rd Party Dependencies:* |cloudpickle|_

    - *Default max_workers:* Not applicable.

    - *Pickling:* This executor takes extra time to pickle and unpickle all
      tasks and their results. If you aren't troubleshooting such issues and
      prefer lower overhead, consider using
      `~pyrseus.executors.inline.InlineExecutor` instead.

    - *OnError handling:* Not applicable.

    Details
    -------

    This variant pickles each submitted task and the task's results, using the
    |cloudpickle|_ module that understands things like lambdas. This is
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

        >>> with CpInlineExecutor() as exe:
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
