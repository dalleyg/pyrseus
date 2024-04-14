"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a simple serial executor,
`~pyrseus.executors.inline.InlineExecutor`, that captures exceptions in the
standard way.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "inline" doesn't pickle, so lambdas are fine.
>>> skip_if_unavailable("inline")
>>> with ExecutorCtx("inline") as exe:
...     assert exe.__class__.__name__ == 'InlineExecutor'
...     assert exe.submit(lambda: os.getpid()).result() == os.getpid()

Plugin-specific Notes
---------------------

- *Common Use Cases:*

  - Light workloads: this plugin is useful for avoiding concurrency overhead
    when running small batches of tasks. This lets developers avoid the
    alternative of rewriting all of their control flow to not use executors at
    all, just to get serial execution.

  - Troubleshooting: since tasks are executed immediately and within the same
    thread, tracing through the task code in a debugger is trivially easy.

- *Concurrency:* This is a non-concurrent, serial-only plugin. All tasks are
  immediately run in the same process and thread they were submitted in.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* This plugin has no 3rd party dependencies.
  Futhermore, it has minimal dependencies to other Pyrseus subpackages.

- *Underlying Executor:* This plugin uses
  `~pyrseus.executors.inline.InlineExecutor`. That executor has no external
  dependencies whatsoever.

- *Default max_workers:* Not applicable.

- *Pickling:* This plugin does not perform any pickling.

- *OnError handling:* Not applicable.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

from typing import Optional

from pyrseus.ctx.simple import SimpleEntryPoint


class EntryPoint(SimpleEntryPoint):
    supports_serial = True
    supports_concurrent = False
    executor_module_name = "pyrseus.executors.inline"
    executor_class_name = "InlineExecutor"

    def create(self, max_workers: Optional[int] = None, **kwargs):
        return self.ExecutorCls(**kwargs), None


ENTRY_POINT = EntryPoint()
