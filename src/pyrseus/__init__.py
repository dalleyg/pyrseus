"""
The `pyrseus` package provides various standalone `~concurrent.futures.Executor`
classes that fill in some gaps in the `concurrent.futures` package.
"""

# Re-export all of the executor classes for easier importing by users.
from pyrseus.executors.cpinline import CpInlineExecutor  # NOQA
from pyrseus.executors.cpnocatch import CpNoCatchExecutor  # NOQA
from pyrseus.executors.cpprocess import CpProcessPoolExecutor  # NOQA
from pyrseus.executors.inline import InlineExecutor  # NOQA
from pyrseus.executors.nocatch import NoCatchExecutor  # NOQA
from pyrseus.executors.pinline import PInlineExecutor  # NOQA
from pyrseus.executors.pnocatch import PNoCatchExecutor  # NOQA

__version__ = "0.2.0"
