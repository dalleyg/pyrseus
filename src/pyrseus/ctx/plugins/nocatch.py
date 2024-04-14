"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a simple serial executor,
`~pyrseus.executors.nocatch.NoCatchExecutor`, that does *not* catch exceptions
in submitted futures.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "nocatch" doesn't catch exceptions at submit time
>>> skip_if_unavailable("nocatch")
>>> def raises():
...     raise RuntimeError(
...         "I am an exception, passed out at submit time, "
...         "instead of held until fut.result() was called."
...     )
>>> with ExecutorCtx("nocatch") as exe:
...     assert exe.__class__.__name__ == 'NoCatchExecutor'
...     exe.submit(raises)                    # <-- NOTE: no fut.result() needed
Traceback (most recent call last):
...
RuntimeError: I am an exception, passed out at submit time,
instead of held until fut.result() was called.

>>> # "nocatch" doesn't pickle, so lambdas are fine.
>>> skip_if_unavailable("nocatch")
>>> with ExecutorCtx("nocatch") as exe:
...     assert exe.submit(lambda: os.getpid()).result() == os.getpid()

Plugin-specific Notes
---------------------

- *Common Use Cases:* for troubleshooting, as a fail-fast variant of the
  `~pyrseus.ctx.plugins.inline`.

- *Concurrency:* This is a non-concurrent, serial-only plugin. All tasks are
  immediately run in the same process and thread they were submitted in.

- *Exceptions:* This plugin has *non-standard* exception-handling semantics: no
  task exceptions are caught and captured in their futures. Exceptions are
  propagated out immediately.

- *3rd Party Dependencies:* This plugin has no 3rd party dependencies.
  Futhermore, it has minimal dependencies to other Pyrseus subpackages.

- *Underlying Executor:* This plugin uses
  `~pyrseus.executors.nocatch.NoCatchExecutor`. That executor has no external
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
    executor_module_name = "pyrseus.executors.nocatch"
    executor_class_name = "NoCatchExecutor"

    def create(self, max_workers: Optional[int] = None, **kwargs):
        return self.ExecutorCls(**kwargs), None


ENTRY_POINT = EntryPoint()
