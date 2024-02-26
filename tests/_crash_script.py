#!/bin/env python3
"""
Helper script for testing the reliability of various multiprocessing backends,
especially ProcessPoolExecutor's multiprocessing.Pool backend. To run this
directly, first run the tests using ``tox``, then you can get help as follows::

    % ./.tox/py/bin/python tests/_crash_script.py --help

See unit tests that run this script for what the expected output is.

.. note:

    This script was adapted from one of Loky's unit test files:
    `test_reusable_executor
    <https://github.com/joblib/loky/blob/master/tests/test_reusable_executor.py>_.
    That file is subject to the following license::

        BSD 3-Clause License

        Copyright (c) 2017, Olivier Grisel & Thomas Moreau. All rights reserved.

        Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions are
        met:

        * Redistributions of source code must retain the above copyright notice,
          this list of conditions and the following disclaimer.

        * Redistributions in binary form must reproduce the above copyright
          notice, this list of conditions and the following disclaimer in the
          documentation and/or other materials provided with the distribution.

        * Neither the name of the copyright holder nor the names of its
          contributors may be used to endorse or promote products derived from
          this software without specific prior written permission.

        THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
        IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
        TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
        PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
        HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
        SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
        TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
        PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
        LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
        NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
        SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import argparse
import ctypes
import faulthandler
import os
import platform
import signal
import sys
import time
from multiprocessing import current_process
from pickle import PicklingError, UnpicklingError

import psutil

from pyrseus.ctx.mgr import ExecutorCtx

# Compat windows
if platform.system() == "Windows":
    from signal import SIGTERM as SIGKILL

    libc = ctypes.cdll.msvcrt
else:
    from ctypes.util import find_library
    from signal import SIGKILL

    libc = ctypes.CDLL(find_library("c"))


def crash():
    """Induces a segfault"""
    faulthandler._sigsegv()


def c_exit(exitcode=0):
    """Induces a libc exit with exitcode 0"""
    libc.exit(exitcode)


def raise_error(etype=UnpicklingError, message=None):
    """Function that raises an Exception in process"""
    raise etype(message)


def invoke(cls):
    """Function that returns a instance of cls"""
    return cls()


class CrashAtPickle:
    """Bad object that triggers a segfault at pickling time."""

    def __reduce__(self):
        crash()


class CrashAtUnpickle:
    """Bad object that triggers a segfault at unpickling time."""

    def __reduce__(self):
        return crash, ()


class ExitAtPickle:
    """Bad object that triggers a segfault at pickling time."""

    def __reduce__(self):
        exit()


class ExitAtUnpickle:
    """Bad object that triggers a process exit at unpickling time."""

    def __reduce__(self):
        return exit, ()


class CExitAtPickle:
    """Bad object that triggers a segfault at pickling time."""

    def __reduce__(self):
        c_exit()


class CExitAtUnpickle:
    """Bad object that triggers a process exit at unpickling time."""

    def __reduce__(self):
        return c_exit, ()


class ErrorAtPickle:
    """Bad object that raises an error at pickling time."""

    def __init__(self, fail=True):
        self.fail = fail

    def __reduce__(self):
        if self.fail:
            raise PicklingError("Error in pickle")
        else:
            return id, (42,)


class ErrorAtUnpickle:
    """Bad object that triggers a process exit at unpickling time."""

    def __init__(self, etype=UnpicklingError, message="the error message"):
        self.etype = etype
        self.message = message

    def __reduce__(self):
        return raise_error, (self.etype, self.message)


class CrashAtGCInWorker:
    """Bad object that triggers a segfault at call item GC time"""

    def __del__(self):
        if current_process().name != "MainProcess":
            crash()


class CExitAtGCInWorker:
    """Exit worker at call item GC time"""

    def __del__(self):
        if current_process().name != "MainProcess":
            c_exit()


def print_step(msg, *, tag="STEP"):
    print(f"{tag}: {msg}")
    sys.stdout.flush()


def timeout_handler(signum, frame):
    if signum == signal.SIGALRM:
        print_step("RECEIVED-SIGALRM")
        parent = psutil.Process(os.getpid())
        children = parent.children(recursive=True)
        for p in children:
            try:
                p.send_signal(SIGKILL)
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(children)
        print_step("TIMED-OUT")
        c_exit()


FUNCS = {
    "c_exit": c_exit,
    "crash": crash,
    "exit": exit,
    "id": id,
    "invoke": invoke,
    "raise": raise_error,
    "sleep": time.sleep,
}

ARG_LISTS = {
    # Created on the main process. For use with "id".
    "c_exit_at_pickle": (CExitAtPickle(),),
    "c_exit_at_unpickle": (CExitAtUnpickle(),),
    "crash_at_pickle": (CrashAtPickle(),),
    "crash_at_unpickle": (CrashAtUnpickle(),),
    "error_at_pickle": (ErrorAtPickle(),),
    "error_at_unpickle": (ErrorAtUnpickle(),),
    "exit_at_pickle": (ExitAtPickle(),),
    "exit_at_unpickle": (ExitAtUnpickle(),),
    # Invoked on workers. For use with "invoke".
    "ctor_c_exit_at_pickle": (CExitAtPickle,),
    "ctor_c_exit_at_unpickle": (CExitAtUnpickle,),
    "ctor_crash_at_pickle": (CrashAtPickle,),
    "ctor_crash_at_unpickle": (CrashAtUnpickle,),
    "ctor_error_at_pickle": (ErrorAtPickle,),
    "ctor_error_at_unpickle": (ErrorAtUnpickle,),
    "ctor_exit_at_pickle": (ExitAtPickle,),
    "ctor_exit_at_unpickle": (ExitAtUnpickle,),
    # Others.
    "na": (),
    "runtime_error": (RuntimeError,),
    "ten": (10,),
    "zero": (0,),
}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("timeout_secs", type=int)
    parser.add_argument("plugin")
    parser.add_argument("func_name", choices=FUNCS)
    parser.add_argument("arg_name", choices=ARG_LISTS)
    parser.add_argument("--max-workers", type=int, default=1)
    args = parser.parse_args(argv)
    func = FUNCS[args.func_name]
    func_args = ARG_LISTS[args.arg_name]
    print_step("PARSED")
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(args.timeout_secs)
    print_step("SET-TIMEOUT")
    ctx = ExecutorCtx(args.plugin, args.max_workers)
    print_step("CONSTRUCTED")
    with ctx as exe:
        print_step("ENTERED-CTX")
        try:
            print_step("ENTERED-TRY")
            fut = exe.submit(func, *func_args)
            print_step("SUBMITTED")
            res = fut.result()
        except Exception:
            print_step("CAUGHT")
            raise  # prints a traceback
        except SystemExit as ex:
            print_step(f"CAUGHT {type(ex).__name__}")
            raise  # no traceback printed for SystemExit
        except BaseException as ex:
            print_step(f"CAUGHT {type(ex).__name__}")
            raise  # presumably prints a traceback
        else:
            print_step(f"RESULT {res}")
    print_step("EXITED-CTX")


if __name__ == "__main__":
    main()
    print_step("EXITING-SCRIPT")
