"""
System-level utilities for the `pyrseus` package.
"""

import importlib
import multiprocessing
import signal
from contextlib import contextmanager
from functools import cache
from typing import Optional

import psutil

LOGICAL_CORES = psutil.cpu_count(logical=True)
PHYSICAL_CORES = psutil.cpu_count(logical=False)  # None on some platforms


def get_num_available_cores(pid: Optional[int] = None, physical: bool = False):
    """
    Returns the number of cores that are available to the given process for
    scheduling threads, respecting the CPU affinity mask when possible. Think of
    this as a better version of `multiprocessing.cpu_count`.

    .. note:

       If you are calling this in code that already depends on the |loky|_
       package, consider using `.loky.backend.context.cpu_count` instead of this
       function. It is even more sophisticated.

    :param pid: process ID to test, on systems that have affinity masks.
        Defaults to the current process.

    :param physical: whether to try only counting the number of physical cores
        instead of logical cores. Silently ignored on platforms that don't
        support querying the physical core count.
    """
    if hasattr(psutil.Process, "cpu_affinity"):
        # Use psutil on any platform that supports it (Linux, Windows, FreeBSD).
        proc = psutil.Process(pid)
        logical_available = len(proc.cpu_affinity())
        if physical:
            if (PHYSICAL_CORES is not None) and (PHYSICAL_CORES >= 0):
                # On most platforms, we can do the right thing.
                return int((PHYSICAL_CORES * logical_available) // LOGICAL_CORES)
            else:
                # On OpenBSD and NetBSD, physical is always None, so give up and
                # just assume it's okay to return the number of available logical
                # cores.
                return logical_available
        else:
            return logical_available
    else:
        # Fallback to just counting the number of visible cores on platforms
        # that don't have Process.cpu_affinity, like macOS.
        return multiprocessing.cpu_count()


@cache
def is_mp_start_method_supported(start_method: str) -> bool:
    """
    Returns whether the given ``start_method`` is supported by the
    `multiprocessing` library, in the current process.
    """
    import multiprocessing as mp

    try:
        mp.get_context(start_method)
    except ValueError:
        return False
    else:
        return True


def module_exists(name):
    """
    Tells whether the given Python module or package exists and looks
    importable, without actually importing it.
    """
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


@contextmanager
def SignalHandlerCtx(signum, handler):
    """
    Safely makes a temporary change to one signal handler.
    """
    unset = object()
    old_handler = unset
    try:
        old_handler = signal.signal(signum, handler)
        yield
    finally:
        if old_handler is not unset:
            signal.signal(signum, old_handler)
