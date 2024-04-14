"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that is a variant of the
`~pyrseus.ctx.plugins.nocatch` one, but this one tests the picklability of all
submitted tasks and their return values, using |cloudpickle|_. This plugin uses
`~pyrseus.executors.cpnocatch.CpNoCatchExecutor`.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "cpnocatch" tests picklability with cloudpickle, e.g. for lambdas
>>> skip_if_unavailable("cpnocatch")
>>> with ExecutorCtx("cpnocatch") as exe:
...     assert exe.__class__.__name__ == 'CpNoCatchExecutor'
...     assert exe.submit(lambda: os.getpid()).result() == os.getpid()

Plugin-specific Notes
---------------------

- *Common Use Cases:* for troubleshooting with extra |cloudpickle|_ testing, as
  a fail-fast variant of the `~pyrseus.ctx.plugins.cpinline`.

- *Concurrency:* This is a non-concurrent, serial-only plugin. All tasks are
  immediately run in the same process and thread they were submitted in.

- *Exceptions:* This plugin has *non-standard* exception-handling semantics: no
  task exceptions are caught and captured in their futures. Exceptions are
  propagated out immediately.

- *3rd Party Dependencies:* |cloudpickle|_

- *Underlying Executor:* `~pyrseus.executors.cpnocatch.CpNoCatchExecutor`

- *Default max_workers:* Not applicable.

- *Pickling:* This plugin takes extra time to pickle and unpickle all tasks and
  their results. If you aren't troubleshooting such issues and prefer lower
  overhead, consider using the `~pyrseus.ctx.plugins.nocatch` plugin instead.

- *OnError handling:* Irrelevant because all tasks are run immediately.

- *OnError handling:* Not applicable.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

from typing import Optional

from pyrseus.ctx.simple import SimpleEntryPoint


class EntryPoint(SimpleEntryPoint):
    supports_serial = True
    supports_concurrent = False
    executor_module_name = "pyrseus.executors.cpnocatch"
    executor_class_name = "CpNoCatchExecutor"
    extra_modules_required = ("cloudpickle",)

    def create(self, max_workers: Optional[int] = None, **kwargs):
        return self.ExecutorCls(**kwargs), None


ENTRY_POINT = EntryPoint()
