"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a factory for the built-in
`~concurrent.futures.ThreadPoolExecutor` that respects the CPU affinity mask
when choosing the default thread count.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "thread" doesn't pickle, so lambdas are fine.
>>> skip_if_unavailable("thread")
>>> with ExecutorCtx("thread") as exe:
...     assert exe.__class__.__name__ == 'ThreadPoolExecutor'
...     assert exe.submit(lambda: os.getpid()).result() == os.getpid()

Plugin-specific Notes
---------------------

- *Common Use Cases:* as replacement for directly using
  `concurrent.futures.ThreadPoolExecutor` for users that enjoy using
  `~pyrseus.ctx.mgr.ExecutorCtx` and/or want to have a more conservative default
  ``max_workers``.

- *Concurrency:* Each worker owns its own thread within the main process.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* This plugin has no 3rd party dependencies.

- *Underlying Executor:* `concurrent.futures.ProcessPoolExecutor`

- *Default max_workers:* Uses `~pyrseus.core.sys.get_num_available_cores`
  instead of `multiprocessing.cpu_count`, respecting the CPU affinity mask when
  possible.

- *Pickling:* Not applicable. For troubleshooting pickling issues, consider
  using one of the serial executors instead, e.g.
  `~pyrseus.ctx.plugins.cpinline`.

- *OnError handling:* Fully supports `~pyrseus.ctx.api.OnError.WAIT` and
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` modes.
  `~pyrseus.ctx.api.OnError.KILL_WORKERS` mode is automatically downgraded to
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES`.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

from pyrseus.ctx.simple import SimpleEntryPoint, shutdown_cancel_on_error


class EntryPoint(SimpleEntryPoint):
    supports_serial = False
    supports_concurrent = True
    executor_module_name = "concurrent.futures"
    executor_class_name = "ThreadPoolExecutor"

    # When there are no exceptions flowing through __exit__, the built-in
    # behavior is correct.
    wrap_for_wait_on_error = lambda self, ctx: (ctx, None)  # NOQA
    # When the user wants upgraded __exit__ semantics to cancel all pending
    # futures on exit, do so.
    wrap_for_cancel_on_error = lambda self, ctx: (ctx, shutdown_cancel_on_error)  # NOQA
    # Silently downgrade kill to cancel since there is no safe and generic way
    # to kill a thread.
    wrap_for_kill_on_error = lambda self, ctx: (ctx, shutdown_cancel_on_error)  # NOQA


ENTRY_POINT = EntryPoint()
