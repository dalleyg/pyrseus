"""
The `pyrseus` package provides the `~pyrseus.ctx.mgr.ExecutorCtx` factory
class, its plugins, and various standalone serial `~concurrent.futures.Executor`
classes. Its goals are to make it easier to troubleshoot executor-based code by
making it easy switch between executor backends, including serial executors
provided by this package.
"""

# Re-export the most useful things.
from pyrseus.ctx.mgr import ExecutorCtx  # NOQA
from pyrseus.executors.cpinline import CpInlineExecutor  # NOQA
from pyrseus.executors.cpnocatch import CpNoCatchExecutor  # NOQA
from pyrseus.executors.cpprocess import CpProcessPoolExecutor  # NOQA
from pyrseus.executors.inline import InlineExecutor  # NOQA
from pyrseus.executors.nocatch import NoCatchExecutor  # NOQA
from pyrseus.executors.pinline import PInlineExecutor  # NOQA
from pyrseus.executors.pnocatch import PNoCatchExecutor  # NOQA

__version__ = "0.1.0"
