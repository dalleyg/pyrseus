"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that uses `.loky`'s enhanced
multiprocessing executors.

Some imports for our tests:

    >>> import os, time
    >>> from pyrseus.ctx.registry import skip_if_unavailable
    >>> from pyrseus.ctx.mgr import ExecutorCtx
    >>> skip_if_unavailable("loky")
    >>> from loky import get_reusable_executor

|loky|_'s executors can be used as an improved backend to the ``"process"`` and
``"cpprocess"`` plugins that use the built-in
`~concurrent.futures.ProcessPoolExecutor`.

    >>> # "loky" can pickle fancy things like lambdas with cloudpickle
    >>> skip_if_unavailable("loky")
    >>> with ExecutorCtx("loky", 1) as exe:
    ...     assert exe.submit(lambda: os.getpid()).result() != os.getpid()

Additionally, the ``"loky"`` plugin allows using |loky|_'s reusable executors,
but with the safety benefits of a context manager. The manager's exit waits till
all tasks submitted via it enter a final state. Any tasks submitted to the
reusable executor through any other means are untouched.

    >>> skip_if_unavailable("loky")
    >>> # Note: protected_exe and unprotected_exe will use the same underlying
    >>> # loky reusable executor.
    >>> unprotected_exe = get_reusable_executor(1)
    >>> with ExecutorCtx("loky", 1, reusable=True) as protected_exe:
    ...     # Submit via the protected context manager.
    ...     # protected_exe.__exit__ will wait for this future to be completed.
    ...     protected_fut = protected_exe.submit(os.getpid)
    ...
    ...     # Submit another, but via the unprotected interface.
    ...     # protected_exe.__exit__ ignores this future.
    ...     unprotected_fut = unprotected_exe.submit(
    ...         # loky handles lambdas fine
    ...         lambda: time.sleep(0.2) or os.getpid()
    ...     )
    >>> # All futures submitted to the ExecutorCtx are completed before exiting.
    >>> assert protected_fut.done()
    >>> # But futures submitted directly to the same underlying reusable
    >>> # executor are unaffected by this. Here we see that unprotected_fut
    >>> # is still sleeping.
    >>> assert not unprotected_fut.done()
    >>> # If we explicitly wait till unprotected_fut is completed too, we can
    >>> # see that it ran on the same worker as protected_fut.
    >>> assert unprotected_fut.result() == protected_fut.result()
    >>> # Clean up must be done explicitly with reusable executors.
    >>> unprotected_exe.shutdown(kill_workers=True)

See `.EntryPoint.create` for more details.

Plugin-specific Notes
---------------------

- *Common Use Cases:* As more robust and more powerful replacement for the
  `~pyrseus.ctx.plugins.process` and `~pyrseus.ctx.plugins.cpprocess` plugins.

- *Concurrency:* Each worker runs in its own process. Unlike
  `~pyrseus.ctx.plugins.process` and `~pyrseus.ctx.plugins.cpprocess`, idle
  workers are automatically killed after a short timeout. This makes it more
  appropriate for interactive use and with workloads that have a low concurrent
  duty cycle.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* |cloudpickle|_ and |loky|_

- *Underlying Executors:* wrappers around various |loky|_ executors.

- *Default max_workers:* uses `~.loky.backend.context.cpu_count`, a smarter
  variant of our `~pyrseus.core.sys.get_num_available_cores`.

- *Pickling:* |cloudpickle|_

- *OnError handling:* Fully supports all `~pyrseus.ctx.api.OnError` modes.

  - To reduce dispatch latency, |loky|_  pre-queues many extra tasks, making
    them be uncancellable; so in `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` mode,
    all of those pre-queued tasks are still run. If this results in too much
    latency, use `~pyrseus.ctx.api.OnError.KILL_WORKERS`.

  - `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` is the default mode for reusable
    executors and `~pyrseus.ctx.api.OnError.KILL_WORKERS` is the default for
    non-reusable ones.

  - In reusable mode, `~pyrseus.ctx.api.OnError.KILL_WORKERS` will kill all
    workers in the pool, not just those working on tasks for one particular
    `~pyrseus.ctx.mgr.ExecutorCtx`. This is the only reason why
    `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` is currently the default in
    reusable mode.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

from functools import cached_property
from typing import Optional, TypeVar

from pyrseus.core.sys import module_exists
from pyrseus.ctx.api import (
    ExecutorPluginEntryPoint,
    OnError,
    OnErrorLike,
    extract_keywords,
)
from pyrseus.ctx.simple import OnExitProxy

Ret = TypeVar("Ret")
"""
Represents the generic return type of a submitted callable.
"""


class EntryPoint(ExecutorPluginEntryPoint):
    supports_serial = False
    supports_concurrent = True

    @cached_property
    def is_available(self) -> bool:
        return module_exists("loky")

    @cached_property
    def allowed_keywords(self):
        from loky import ProcessPoolExecutor, get_reusable_executor

        return (
            extract_keywords(get_reusable_executor)
            | extract_keywords(ProcessPoolExecutor)
            | extract_keywords(self.create)
        )

    def create(
        self,
        max_workers: Optional[int] = None,
        *,
        reusable: bool = False,
        on_error: Optional[OnErrorLike] = None,
        **kwargs,
    ):
        """
        Creates a |loky|_ executor.

        As detailed below, this factory first asks |loky|_ for an appropriate
        executor instance. We then wrap that instance in `.OnExitProxy` to imbue
        it with the ``__exit__`` time behavior that other Pyrseus plugins
        provide.

        - If ``reusable`` is false, the executor is obtained via
          `.loky.ProcessPoolExecutor`.

          - If supplied, ``reuse`` and ``kill_workers`` keyword arguments are
            silently ignored (as is standard practice for unused keyword
            arguments with `~pyrseus.ctx.mgr.ExecutorCtx`).

          - If ``on_error`` is ``None``, it's treated as if it was
            `.OnError.CANCEL_FUTURES`. Regardless, ``on_error`` is consumed by
            this factory method.

          - All other arguments are passed to the underlying executor's
            constructor, as-is.

          - An `.OnExitProxy` will be created with ``shutdown=False``.

        - If ``reusable`` is true, the executor is obtained by
          `.loky.get_reusable_executor`.

          - If ``on_error`` is ``None``, it's treated as if it was
            `.OnError.KILL_WORKERS`. Regardless, ``on_error`` is consumed by
            this factory method.

          - All other arguments are passed to the underlying executor's
            constructor, as-is.

          - An `.OnExitProxy` will be created with ``shutdown=True``.

        :param max_workers: the maximum number of workers to use in the pool.
            This is passed directly to |loky|_ since it has a smart algorithm
            for avoiding oversubscription of CPU resources.

        :param reusable: decides whether to share a worker pool with other
            contexts or not. Reusable pools can have better startup time, but by
            default they can block for arbitrary amounts of time if there was an
            uncaught exception within the context.

        :param on_error: controls what to do with the submitted futures and the
            worker pool if there was an uncaught exception within the context.
            See `.OnExitProxy` and `.OnError` for details.
        """
        from loky import ProcessPoolExecutor, get_reusable_executor

        # Don't do anything special here when max_workers=None. loky's defaults
        # are even better than what we do in process and cpprocess, e.g. it also
        # tries to respect cgroup limits.

        if reusable:
            # CANCEL_FUTURES can be especially slow with loky reusable executors
            # because it pre-loads a large work queue with tasks. But it's a
            # safer approach in reusable mode. We choose to be safe here since
            # reusable mode is opt-in.
            if on_error is None:
                on_error = OnError.CANCEL_FUTURES
            else:
                on_error = OnError.get(on_error)
            # In reusable mode, we don't own the whole worker pool, so we setup
            # a proxy that gives the executor context manager semantics with
            # efforts taken to avoid interfering with futures that were not
            # submitted by this proxy instance.
            shutdown = False
            exe = get_reusable_executor(max_workers, **kwargs)
        else:
            # In non-reusable mode, we own the whole worker pool, so it's okay
            # to use shutdown=True. Alternatively, we could replicate the logic
            # in SimpleEntryPoint.create, but we choose to use OnExitProxy here
            # too for consistency.
            #
            # But first, we need to remove any reusable-only arguments. There
            # are only two right now, so we do it by hand.
            kwargs.pop("reuse", None)
            kwargs.pop("kill_workers", None)
            # It's safe enough to be aggressive since we own the whole pool.
            if on_error is None:
                on_error = OnError.KILL_WORKERS
            else:
                on_error = OnError.get(on_error)
            # This mimics what SimpleEntryPoint.create does, but specialized for
            # loky. Loky's shutdown method doesn't natively support cancel mode,
            # so we do still have to emulate that case.
            shutdown = on_error is not OnError.CANCEL_FUTURES
            ctx = ProcessPoolExecutor(max_workers, **kwargs)
            exe = ctx.__enter__()  # just to be pedantic

        proxy = OnExitProxy(exe, on_error=on_error, shutdown=shutdown)
        return proxy, None


ENTRY_POINT = EntryPoint()
