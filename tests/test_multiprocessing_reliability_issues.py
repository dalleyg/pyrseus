"""
Tests the built-in `multiprocessing.Pool`'s robustness to crashes. This was
adapted from one of Loky's unit test files: `test_reusable_executor
<https://github.com/joblib/loky/blob/master/tests/test_reusable_executor.py>_.
That file is subject to the following license::

    BSD 3-Clause License

    Copyright (c) 2017, Olivier Grisel & Thomas Moreau.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
    list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.

    * Neither the name of the copyright holder nor the names of its
    contributors may be used to endorse or promote products derived from
    this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import os.path
import platform
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import pytest

from pyrseus.ctx.registry import skip_if_unavailable

# These tests intentionally trigger deadlocks in Python's multiprocessing
# library, so they're slow to run. We skip them all in CI runs.
#
# These tests rely on SIGALRM. Unfortunately, that doesn't exist on Windows.
if platform.system() == "Windows":
    pytestmark = [
        pytest.mark.skip("SIGALRM does not exist on Windows"),
        pytest.mark.slow,
    ]
else:
    pytestmark = pytest.mark.slow


@dataclass
class Case:
    func_name: str
    arg_name: str
    last_step: str
    errors: Optional[Union[str, Tuple[str]]] = None
    loky_errors: Optional[Union[str, Tuple[str]]] = None
    timeout_secs: int = 2

    @property
    def eff_errors(self):
        if self.errors is None:
            return ()
        elif isinstance(self.errors, str):
            return (self.errors,)
        elif isinstance(self.errors, tuple):
            return self.errors
        else:
            return TypeError(type(self.errors))


KNOWN_ERROR_STRINGS = (
    "BrokenProcessPool",
    "PicklingError",
    # "RemoteTraceback",
    "Segmentation fault",
    "SystemExit",
    "UnpicklingError",
)

CASES: List[Union[Case, Dict[Optional[str], Case]]] = [
    # fmt: off

    # verifies that the timeouts work
    Case("sleep", "zero", "EXITING-SCRIPT", timeout_secs=1),
    Case("sleep", "ten",  "TIMED-OUT",      timeout_secs=1),

    # Check problem occurring while pickling a task in
    Case("id", "error_at_pickle", "CAUGHT", ("RemoteTraceback", "PicklingError")),
    {
        'loky': Case("id", "exit_at_pickle", "CAUGHT", ('RemoteTraceback', 'SystemExit', 'PicklingError')),
        None:   Case("id", "exit_at_pickle", "TIMED-OUT"),
    },
    # The following two are disabled for now.
    #  - With "process", the test deadlocks, and it leaves 1 orphaned process
    #    even after manually killing the test.
    #  - With "loky", the same problems exist, but 3 orphans are left over.
    #
    # - Case("id", "c_exit_at_pickle", "TIMED-OUT"),
    # - Case("id", "crash_at_pickle",  "TIMED-OUT"),

    # Check problem occurring while unpickling a task on workers
    Case("id", "error_at_unpickle", "CAUGHT", ("UnpicklingError", "BrokenProcessPool")),
    {
        'loky': Case("id", "exit_at_unpickle", "CAUGHT", ("RemoteTraceback", "SystemExit", "BrokenProcessPool")),
        None:   Case("id", "exit_at_unpickle", "CAUGHT", "BrokenProcessPool"),
    },
    Case("id", "c_exit_at_unpickle", "CAUGHT", "BrokenProcessPool"),
    {
        'loky': Case("id", "crash_at_unpickle", "CAUGHT", "TerminatedWorkerError"),
        None:   Case("id", "crash_at_unpickle", "CAUGHT", "BrokenProcessPool"),
    },

    # Check problem occurring during function execution on workers
    Case("raise",  "runtime_error", "CAUGHT", ("RemoteTraceback", "RuntimeError")),
    Case("exit",   "na",            "CAUGHT", "SystemExit"),
    Case("c_exit", "na",            "CAUGHT", "BrokenProcessPool"),
    {
        'loky': Case("crash", "na", "CAUGHT", "TerminatedWorkerError"),
        None:   Case("crash", "na", "CAUGHT", "BrokenProcessPool"),
    },

    # Check problem occurring while pickling a task result on workers
    Case("invoke", "ctor_error_at_pickle",  "CAUGHT", ("RemoteTraceback", "PicklingError")),
    Case("invoke", "ctor_exit_at_pickle",   "CAUGHT", "SystemExit"),
    Case("invoke", "ctor_c_exit_at_pickle", "CAUGHT", "BrokenProcessPool"),
    {
        'loky': Case("invoke", "ctor_crash_at_pickle", "CAUGHT", "TerminatedWorkerError"),
        None:   Case("invoke", "ctor_crash_at_pickle", "CAUGHT", "BrokenProcessPool"),
    },

    # Check problem occurring while unpickling a task in the result handler
    Case("invoke", "ctor_error_at_unpickle", "CAUGHT", ("RemoteTraceback", "UnpicklingError", "BrokenProcessPool")),
    Case("invoke", "ctor_exit_at_unpickle",  "CAUGHT", ("RemoteTraceback", "SystemExit", "BrokenProcessPool")),
    # fmt: on
]


@pytest.mark.parametrize("case", list(range(len(CASES))))
@pytest.mark.parametrize("plugin", ("process", "loky"))
def test_crash_scenarios(plugin: str, case: int):
    skip_if_unavailable(plugin)

    case = CASES[case]
    if isinstance(case, dict):
        case = case.get(plugin, case[None])

    # Run the simple executor test.
    cmd = [
        sys.executable,
        "_crash_script.py",
        str(case.timeout_secs),
        plugin,
        case.func_name,
        case.arg_name,
    ]
    res = subprocess.run(
        cmd,
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=case.timeout_secs + 5,  # an extra layer of timeout protection
        text=True,
    )

    # Quick checks and early exit if we expected it to succeed.
    if case.last_step == "EXITING-SCRIPT":
        assert res.returncode == 0
        assert res.stdout.rstrip().endswith("STEP: EXITING-SCRIPT")
        for s in KNOWN_ERROR_STRINGS:
            assert s not in res.stdout, s
        return

    # More involved checks for the normal case of expecting some kind of
    # failure.
    msgs = []

    last_step = None
    lines = res.stdout.splitlines()
    for line in lines:
        if line.startswith("STEP: "):
            last_step = line.split()[1]
    if last_step != case.last_step:
        msgs.append(f"Last step: expected={case.last_step} actual={last_step}")

    errors = case.eff_errors
    for error in errors:
        if error not in res.stdout:
            msgs.append(
                f"Expected {error} in the test script's output, "
                f"but it was not there."
            )

    for error in sorted(set(KNOWN_ERROR_STRINGS) - set(errors)):
        if error in res.stdout:
            msgs.append(f"Unexpected error {error} in the test script's output.")

    if msgs:
        msgs.insert(0, "Unexpected result")
        msgs.append("Command:")
        msgs.append(" ".join(cmd))
        msgs.append("Script output:")
        msgs.append(res.stdout)
        pytest.fail("\n".join(msgs))
