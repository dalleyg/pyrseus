# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown] editable=true slideshow={"slide_type": ""} tags=["pyflyby-cell"]
# # ExecutorCtx vs. Classes
#
# In this notebook, we explore how `ExecutorCtx` can make it easier to switch
# between different executor classes, relative to directly using those classes.

# %% [markdown]
# ## Setup
#
# Before running this notebook, make sure everything it depends on is installed:
#
#     # Modify the docs/requirements.txt path if you're running this
#     # command from anything except the repository's root directory.
#     python -m pip install -r docs/requirements.txt
#
# These are some imports that we'll use throughout this notebook:

# %% editable=true slideshow={"slide_type": ""} tags=["pyflyby-cell"]
import sys

sys.path.append("../../../src")  # assume we're running it from a Pyrseus source clone
import os
from socket import gethostname
from threading import get_ident

# %% [markdown]
# Here's a helper function that we'll use in each of the examples
# that features a valid executor.


# %%
def infer_exe_type(exe):
    """
    Runs a few tasks to infer what major type of executor is being used.
    """
    # We use multiple tasks in this notebook to avoid needing to
    # discuss pickle vs. cloudpickle issues.
    if exe.submit(gethostname).result() != gethostname():
        return "Using a multi-host executor."
    elif exe.submit(os.getpid).result() != os.getpid():
        return "Using a multi-process executor."
    elif exe.submit(get_ident).result() != get_ident():
        return "Using a multi-threaded executor."
    else:
        return "Using a serial executor."


# %% [markdown]
# ## Using Explicit Classes
#
# Suppose we have some initial concurrent code that uses `ProcessPoolExecutor`
# with a few constructor arguments.

# %%
# Reasonable initial code.
from concurrent.futures import ProcessPoolExecutor  # NOQA
from multiprocessing import get_context  # NOQA

with ProcessPoolExecutor(2, mp_context=get_context("spawn")) as exe:
    # All of the with-statement bodies are trivial in this notebook so
    # that we can concentrate on the different ways of constructing
    # the executors.
    print(infer_exe_type(exe))

# %% [markdown]
# Now suppose that we want to run that code serially. Then we need to change
# all of the setup lines, without making any mistakes:
# - change the imports,
# - change the executor class used in the with-statement,
# - remove the `max_workers` argument, and
# - remove the `mp_context` argument.

# %%
# Switching executors required a lot of changes.
from pyrseus.executors.inline import InlineExecutor  # NOQA

with InlineExecutor() as exe:
    print(infer_exe_type(exe))

# %% [markdown]
# If debugging, we might then start commenting out lines to switch back and
# forth, making it even messier e.g.:

# %%
# Switching back and forth while debugging can be cumbersome.
from concurrent.futures import ProcessPoolExecutor  # NOQA
from multiprocessing import get_context  # NOQA

from pyrseus.executors.inline import InlineExecutor  # NOQA

# with ProcessPoolExecutor(2, mp_context=get_context("spawn")) as exe:
with InlineExecutor() as exe:
    print(infer_exe_type(exe))

# %% [markdown]
# Or we might take the extra effort to do it one of the "right" ways, at the
# cost of yet more code clutter:

# %%
# Switching back and forth a "right" way clutters the code.
mode = "inline"  # manually edited while debugging to switch modes
from concurrent.futures import ProcessPoolExecutor  # NOQA
from contextlib import ExitStack  # NOQA
from multiprocessing import get_context  # NOQA

from pyrseus.executors.inline import InlineExecutor  # NOQA

with ExitStack() as es:
    # Complicated!
    if mode == "inline":
        exe = es.enter_context(InlineExecutor())
    elif mode == "process":
        exe = es.enter_context(ProcessPoolExecutor(2, mp_context=get_context("spawn")))
    else:
        raise ValueError(mode)
    # As before...
    print(infer_exe_type(exe))

# %% [markdown]
# ## Using ExecutorCtx with Explicit Plugins
#
# If we use `ExecutorCtx` instead, then we can keep the simplicity of the
# original code while also making it trivial to switch between different
# executor classes.

# %%
# ExecutorCtx variant of the initial code. This looks very
# similar to the "Explicit Classes" variant.
from multiprocessing import get_context  # NOQA

from pyrseus.ctx.mgr import ExecutorCtx  # NOQA

with ExecutorCtx("process", 2, mp_context=get_context("spawn")) as exe:
    print(infer_exe_type(exe))

# %% [markdown]
# Switching to `InlineExecutor` is just a matter of changing the first argument
# to `ExecutorCtx`. The context manager knows that `InlineExecutor` doesn't
# care about the positional `max_workers` argument or the keyword `mp_context`
# argument, so it silently hides them from `InlineExecutor`.

# %%
# All we had to change was the plugin name. Nothing else.
from multiprocessing import get_context  # NOQA

from pyrseus.ctx.mgr import ExecutorCtx  # NOQA

with ExecutorCtx("inline", 2, mp_context=get_context("spawn")) as exe:
    print(infer_exe_type(exe))

# %% [markdown]
# Alternatively, it would still be fine to remove those extra arguments if we
# don't anticipate going back and using `ProcessPoolExecutor` anymore.

# %%
# It's also okay to remove the extra arguments if we won't be using the
# "process" plugin anymore.
with ExecutorCtx("inline") as exe:
    print(infer_exe_type(exe))

# %% [markdown]
# `ExecutorCtx` is nice and will complain about keyword arguments that no
# available plugin accepts. Furthermore, it tells us what the available
# plugins and allowed keywords are. That way we can see if the problem
# is a missing plugin, a mistyped keyword, etc.

# %%
# But ExecutorCtx does help identify keywords that are not accepted by any
# plugin.
try:
    ExecutorCtx(
        "inline",
        # max_workers=2 is silently ignored because it could be relevant for a
        # concurrent plugin.
        2,
        # mp_context is silently ignored because some plugins like "process" can
        # use it.
        mp_context=get_context("spawn"),
        # But ExecutorCtx is nice and complains about this keyword that is
        # always ignored.
        not_a_keyword=42,
    )
except Exception as ex:
    print("CAUGHT EXCEPTION:", ex)

# %% [markdown]
# ## Using ExecutorCtx with Implicit Plugins
#
# Another way to use `ExecutorCtx` is to rely on its system for managing a
# default serial and a default concurrent plugin. If we just care about
# switching between those two defaults, then we just need to *exclude* the
# plugin name argument and change the `max_workers` argument.
#
# Here's the analogous initial code snippet.

# %%
# In this snippet, we just exclude "process".
from multiprocessing import get_context  # NOQA

from pyrseus.ctx.mgr import ExecutorCtx  # NOQA

with ExecutorCtx(2, mp_context=get_context("spawn")) as exe:
    print(infer_exe_type(exe))

# %% [markdown]
# Then by changing `2` to `0`, we'll implicitly use `InlineExecutor`, assuming
# it's the default.

# %%
# Now we're using InlineExecutor, just by changing 2 to 0.
from multiprocessing import get_context  # NOQA

from pyrseus.ctx.mgr import ExecutorCtx  # NOQA

with ExecutorCtx(0, mp_context=get_context("spawn")) as exe:
    print(infer_exe_type(exe))

# %% [markdown]
# See the "Plugins for ExecutorCtx" documentation page for the various ways
# of changing the default plugins.
