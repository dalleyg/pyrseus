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

    Summary
    -------

    - *Common Use Cases:* For the same use cases as
      `concurrent.futures.ProcessPoolExecutor`, but when users wish to use
      |cloudpickle|_ instead of `pickle` for serializing tasks and their
      results.

    - *Concurrency:* Each worker runs in its own process.

    - *Exceptions:* This plugin has standard exception-handling semantics: all
      task-related exceptions are captured in the task's future.

    - *3rd Party Dependencies:* |cloudpickle|_

    - *Default max_workers*:* Uses `~pyrseus.core.sys.get_num_available_cores`
      instead of `multiprocessing.cpu_count`, respecting the CPU affinity mask
      when possible. Currently is unaware of cgroups constraints.

    - *Pickling:* |cloudpickle|_

    - *OnError handling:* Fully supports `~pyrseus.ctx.api.OnError.WAIT` and
      `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` modes.
      `~pyrseus.ctx.api.OnError.KILL_WORKERS` mode is automatically downgraded
      to `~pyrseus.ctx.api.OnError.CANCEL_FUTURES`.

      - By default, `~concurrent.futures.ProcessPoolExecutor`, pre-queues one
        extra task, making it uncancellable; so in
        `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` mode, that pre-queued task
        will still be run.

    Details
    -------

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
