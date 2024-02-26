"""
Tests some basic features of `pyrseus.interactive.get_executor`.
"""

import os
import sys
from threading import get_ident

from pyrseus.ctx.registry import skip_if_unavailable
from pyrseus.interactive import get_executor


def test_can_create_default_serial_exe():
    exe = get_executor(0)
    assert exe.submit(os.getpid).result() == os.getpid()
    assert exe.submit(get_ident).result() == get_ident()
    # Make sure there are no leaked references. 1 count is from our local
    # variable. The other count is from the argument to sys.getrefcount.
    assert sys.getrefcount(exe) == 2


def test_can_create_default_concurrent_exe():
    exe = get_executor(1)
    assert exe.submit(os.getpid).result() != os.getpid()
    # Make sure there are no leaked references. 1 count is from our local
    # variable. The other count is from the argument to sys.getrefcount.
    assert sys.getrefcount(exe) == 2


def test_loky_reuse():
    """
    Verifies that loky's reusable feature works properly, even with all of the
    wrapper layers we've added.
    """
    skip_if_unavailable("loky")
    exe0 = get_executor("loky", 1, reusable=True)
    exe1 = get_executor("loky", 1, reusable=True)
    assert exe0._obj._exe is exe1._obj._exe
