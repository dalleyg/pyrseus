"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that is a variant of the
`~pyrseus.ctx.plugins.nocatch` one, but this one tests the picklability of all
submitted tasks and their return values, using `pickle`. This plugin uses
`~pyrseus.executors.pnocatch.PNoCatchExecutor`.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "pnocatch" tests picklability of normal tasks and return values
>>> skip_if_unavailable("pnocatch")
>>> with ExecutorCtx("pnocatch") as exe:
...     assert exe.__class__.__name__ == 'PNoCatchExecutor'
...     assert exe.submit(os.getpid).result() == os.getpid()

>>> # "pnocatch" can help find pickling problems that concurrent executors have
>>> skip_if_unavailable("pnocatch")
>>> with ExecutorCtx("pnocatch") as exe:
...     exe.submit(lambda: os.getpid())       # <-- NOTE: no fut.result() needed
Traceback (most recent call last):
...
_pickle.PicklingError: Can't pickle ...

Plugin-specific Notes
---------------------

- *Common Use Cases:* for troubleshooting with extra `pickle` testing, as a
  fail-fast variant of the `~pyrseus.ctx.plugins.pinline`.

- *Concurrency:* This is a non-concurrent, serial-only plugin. All tasks are
  immediately run in the same process and thread they were submitted in.

- *Exceptions:* This plugin has *non-standard* exception-handling semantics: no
  task exceptions are caught and captured in their futures. Exceptions are
  propagated out immediately.

- *3rd Party Dependencies:* This plugin has no 3rd party dependencies.

- *Underlying Executor:* `~pyrseus.executors.pnocatch.PNoCatchExecutor`

- *Default max_workers:* Not applicable.

- *Pickling:* This plugin takes extra time to pickle and unpickle all tasks and
  their results. If you aren't troubleshooting such issues and prefer lower
  overhead, consider using the `~pyrseus.ctx.plugins.nocatch` plugin instead.

- *OnError handling:* Not applicable.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

from functools import cached_property
from typing import Optional, Set

from pyrseus.core.pickle import get_round_trip_keywords
from pyrseus.ctx.simple import SimpleEntryPoint


class EntryPoint(SimpleEntryPoint):
    supports_serial = True
    supports_concurrent = False
    executor_module_name = "pyrseus.executors.pnocatch"
    executor_class_name = "PNoCatchExecutor"

    @cached_property
    def allowed_keywords(self) -> Set[str]:
        return get_round_trip_keywords()

    def create(self, max_workers: Optional[int] = None, **kwargs):
        return self.ExecutorCls(**kwargs), None


ENTRY_POINT = EntryPoint()
