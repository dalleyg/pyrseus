"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that is a variant of the
`~pyrseus.ctx.plugins.inline` one, but this one tests the picklability of all
submitted tasks and their return values, using |cloudpickle|_. This plugin uses
`~pyrseus.executors.cpinline.CpInlineExecutor`.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "cpinline" tests picklability with cloudpickle, e.g. for lambdas
>>> skip_if_unavailable("cpinline")
>>> with ExecutorCtx("cpinline") as exe:
...     assert exe.__class__.__name__ == 'CpInlineExecutor'
...     assert exe.submit(lambda: os.getpid()).result() == os.getpid()

Plugin-specific Notes
---------------------

- *Common Use Cases:* serially troubleshooting pickling problems for
  multi-process plugins that use |cloudpickle|_ for serialization.

- *Concurrency:* This is a non-concurrent, serial-only plugin. All tasks are
  immediately run in the same process and thread they were submitted in.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* |cloudpickle|_

- *Underlying Executor:* `~pyrseus.executors.cpinline.CpInlineExecutor`

- *Default max_workers:* Not applicable.

- *Pickling:* This plugin takes extra time to pickle and unpickle all tasks and
  their results. If you aren't troubleshooting such issues and prefer lower
  overhead, consider using the `~pyrseus.ctx.plugins.inline` plugin instead.

- *OnError handling:* Not applicable.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

from typing import Optional

from pyrseus.ctx.simple import SimpleEntryPoint


class EntryPoint(SimpleEntryPoint):
    supports_serial = True
    supports_concurrent = False
    executor_module_name = "pyrseus.executors.cpinline"
    executor_class_name = "CpInlineExecutor"
    extra_modules_required = ("cloudpickle",)

    def create(self, max_workers: Optional[int] = None, **kwargs):
        return self.ExecutorCls(**kwargs), None


ENTRY_POINT = EntryPoint()
