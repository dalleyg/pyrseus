"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that is a variant of the
`~pyrseus.ctx.plugins.inline` one, but this one tests the picklability of all
submitted tasks and their return values, using `pickle`. This plugin uses
`~pyrseus.executors.pinline.PInlineExecutor`.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "pinline" tests picklability of normal tasks and return values
>>> skip_if_unavailable("pinline")
>>> with ExecutorCtx("pinline") as exe:
...     assert exe.__class__.__name__ == 'PInlineExecutor'
...     assert exe.submit(os.getpid).result() == os.getpid()

>>> # "pinline" can help find pickling problems that concurrent executors have
>>> skip_if_unavailable("pinline")
>>> with ExecutorCtx("pinline") as exe:
...     exe.submit(lambda: os.getpid()).result()
Traceback (most recent call last):
...
_pickle.PicklingError: Can't pickle ...

Plugin-specific Notes
---------------------

- *Common Use Cases:* troubleshooting pickling problems for other plugins that
  use `pickle` for serialization.

- *Concurrency:* This is a non-concurrent, serial-only plugin. All tasks are
  immediately run in the same process and thread they were submitted in.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* This plugin has no 3rd party dependencies.

- *Underlying Executor:* `~pyrseus.executors.pinline.PInlineExecutor`

- *Default max_workers:* Not applicable.

- *Pickling:* This plugin takes extra time to pickle and unpickle all tasks and
  their results. If you aren't troubleshooting such issues and prefer lower
  overhead, consider using the `~pyrseus.ctx.plugins.inline` plugin instead.

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
    executor_module_name = "pyrseus.executors.pinline"
    executor_class_name = "PInlineExecutor"

    @cached_property
    def allowed_keywords(self) -> Set[str]:
        return get_round_trip_keywords()

    def create(self, max_workers: Optional[int] = None, **kwargs):
        return self.ExecutorCls(**kwargs), None


ENTRY_POINT = EntryPoint()
