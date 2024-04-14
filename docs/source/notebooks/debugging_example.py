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
# # Debugging Tasks with Pyrseus
#
# In this notebook, we show how Pyrseus can help troubleshoot problems with
# tasks, especially problems with pickling.

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

# %%
import sys

sys.path.append("../../../src")  # assume we're running it from a Pyrseus source clone
import pickle
import random
from multiprocessing import get_context

import cloudpickle

from pyrseus.core.pickle import call_with_round_trip_pickling, try_pickle_round_trip
from pyrseus.ctx.mgr import ExecutorCtx

# %% [markdown]
# And here's a simple custom function we'll be experimenting with.
# It works like `sorted`, but it uses the (slow) Selection Sort algorithm.


# %%
def selection_sort(data):
    """
    Simple selection sort.

    Adapted from: https://en.wikipedia.org/wiki/Selection_sort
    """
    # Make a shallow copy of the data so that this can be a
    # non-mutating function.
    ret = list(data)

    # Now perform the selection sort.
    for i in range(len(data)):
        j_min = i
        for j in range(i + 1, len(data)):
            if ret[j] < ret[j_min]:
                j_min = j
        if j_min != i:
            ret[j_min], ret[i] = ret[i], ret[j_min]

    return ret


# %% [markdown]
# ## Non-executor Usage
#
# Let's first try out the function by calling it directly with a few hand-crafted test cases.

# %%
for data in (
    (),
    (1,),
    (1, 2),
    (1, 2, 3),
    (1, 2, 3, 4),
    (1, 2, 3, 4, 5),
    (1, 5, 2, 4, 3),
    (5, 4, 3, 2, 1),
    (4, 3, 2, 1),
    (3, 2, 1),
):
    expected = sorted(data)
    actual = selection_sort(data)
    assert actual == expected, (data, actual, expected)
    print(f"{str(data):<15s} -> {actual}")


# %% [markdown]
# Let's also create a randomized test helper function and run it
# on a few different inputs.


# %%
def sorting_test_with_big_random_list(seed, n=1000, min_int=0, max_int=500):
    # Make this test repeatable.
    random.seed(seed)
    # Generate some random data.
    data = [random.randint(min_int, max_int) for _ in range(n)]
    # Sort with our method.
    actual = selection_sort(data)
    # Sort with a known good implementation.
    expected = sorted(data)
    # Tell whether the two match.
    return actual == expected
    if actual != expected:
        raise ValueError(f"Results for: {seed=}, {n=}, {min_int=}, {max_int=}")


assert sorting_test_with_big_random_list(0)
assert sorting_test_with_big_random_list(42)

# %% [markdown]
# ## Failures with ProcessPoolExecutor
#
# Now suppose we want to run that test helper many times in parallel, using
# `ProcessPoolExecutor`. Unfortunately, we quickly run into trouble.
# Depending on your Python version, your platform, and exactly what was
# submitted, this will result in at least one of the following:
#  - dead workers (undesirable),
#  - workers printing messages to stderr (undesirable),
#  - `BrokenProcessPool` exceptions (a symptom),
#  - exceptions talking about `'__main__'` (a symptom),
#  - exceptions talking about pickling (the real problem), and/or
#  - exceptions talking about unpickling (a symptom).
#
# For the sake of this notebook, we force it to use the most widely supported
# pool type (`"spawn"`). At least on Python 3.10, this type also results in
# the most verbose and confusing output.

# %% editable=true slideshow={"slide_type": ""} tags=["raises-exception"]
# In this cell, we first encounter a problem running our function
# in some multiprocessing workers.
try:
    print("This test should print out many stderr lines from the workers, but succeed.")
    sys.stdout.flush()

    with ExecutorCtx("process", 4, mp_context=get_context("spawn")) as exe:
        futs = [
            exe.submit(sorting_test_with_big_random_list, seed) for seed in range(25)
        ]
        for seed, fut in enumerate(futs):
            if not fut.result():
                print(f"Seed {seed} failed.")

except Exception as ex:
    print("CAUGHT EXCEPTION (expected):", ex)
else:
    raise RuntimeError("An exception should have been thrown.")

# %% [markdown]
# ## Troubleshooting with Serial Executors
#
# In cases where the exception message doesn't make it clear to the user
# what to do, a common strategy is to run the code serially. Fortunately,
# `ExecutorCtx` makes this trivial to do: we just need to change the plugin
# name (or the `max_workers` argument if the plugin name is being omitted).

# %%
# In this cell, we've failed to replicate the problem.
#
# Notice that all we had to do was change "process"
# to "inline" to try this out. We didn't need to remove
# the 4 or mp_context arguments.
with ExecutorCtx("inline", 4, mp_context=get_context("spawn")) as exe:
    futs = [exe.submit(sorting_test_with_big_random_list, seed) for seed in range(25)]
    for seed, fut in enumerate(futs):
        if not fut.result():
            print(f"Seed {seed} failed.")

# %% [markdown]
# Unfortunately, the above snippet doesn't reproduce the problem. Let's
# assume this led to us doing some more experiments and/or web searches,
# making us think this could be related to pickling and/or unpickling.
#
# At this point, we may try using `PInlineExecutor`, since it advertises
# itself as a tool for troubleshooting pickling problems. And indeed we
# now have a reproducer.

# %%
# In this cell, we have successfully replicated the problem with a serial executor.
#
# Notice that all we had to do was change "inline"
# to "pinline" to try this out.
try:
    with ExecutorCtx("pinline", 4, mp_context=get_context("spawn")) as exe:
        futs = [
            exe.submit(sorting_test_with_big_random_list, seed) for seed in range(25)
        ]
        for seed, fut in enumerate(futs):
            if not fut.result():
                print(f"Seed {seed} failed.")

except Exception as ex:
    print("CAUGHT EXCEPTION (expected):", ex)

# %% [markdown]
# Additionally, this test was done with the pure Python pickler,
# so we could even trace into it with ``ipdb`` if we want.

# %%
# If you'd like to try debugging it yourself, then
#  (a) remove the try-except wrapper around the previous cell,
#  (b) uncomment the %debug line from this cell, and
#  (c) run both cells, one at a time.

# # %debug

# %% [markdown]
# ## Testing Cloudpickle Serially
#
# At this point, we have figured out it's a picklability problem. Hopefully we've also
# figured out that the problem is that we're using a function that's defined in
# ``__main__`` instead of in an imported module. Additionally, we've hopefully heard
# about the `cloudpickle` library being a solution to this kind of problem.
#
# We could test this last hypothesis in a few ways. First, let's verify whether
# `cloudpickle` works at all on our function. It does:

# %%
# cloudpickle says it can handle our function, so we have a chance.
pickled = cloudpickle.dumps(sorting_test_with_big_random_list, -1)
reconstructed = cloudpickle.loads(pickled)
assert reconstructed(0)

# %% [markdown]
# That said, we probably shouldn't trust `cloudpickle` that much since `pickle` thought
# it could handle our function too (and technically it can, but only when we don't
# send the pickled bytestring to another process for unpickling).
#
# Fortunately, Pyrseus ships with a simple test function that simulates this situation.
# First let's show that we can replicate the problem with it when using `pickle`.

# %%
# First, make sure that try_pickle_round_trip can replicate our problem when using pickle.
try:
    try_pickle_round_trip(
        sorting_test_with_big_random_list,
        dumps=pickle.dumps,
        loads=pickle.loads,
        hide_main=True,  # The default is true. We include it here for emphasis.
    )
except Exception:
    print("try_pickle_round_trip successfully replicated the problem with pickle.")
else:
    raise RuntimeError("try_pickle_round_trip failed to replicate the problem.")

# %% [markdown]
# Now let's try using that function to see it thinks `cloudpickle` will fix our problems.

# %%
# Indeed, try_pickle_round_trip tells us that if we use cloudpickle,
# then our pickling problems will likely go away.
reconstructed = try_pickle_round_trip(
    sorting_test_with_big_random_list,
    dumps=cloudpickle.dumps,
    loads=cloudpickle.loads,
)
assert reconstructed(0)

# %% [markdown]
# We might also try using an even more complete tester that internally:
# - runs `try_pickle_round_trip` on the function (similar to above),
# - calls the function (so we see if the call itself is a problem), and
# - runs `try_pickle_round_trip` on the function result (in case there's a picklability problem with it).

# %%
# call_with_round_trip_pickling also thinks that everything's good if we
# switch to using cloudpickle.
assert call_with_round_trip_pickling(
    sorting_test_with_big_random_list,
    args=(0,),
    kwargs={},
    dumps=cloudpickle.dumps,
    loads=cloudpickle.loads,
)

# %% [markdown]
# ## Trying a Cloudpickle-enabled Executor
#
# So now let's try some `cloudpickle`-enabled executors.
#
# First, we see that `CpProcessPoolExecutor` works fine. It's just
# a thin wrapper around `ProcessPoolExecutor` that uses `cloudpickle`
# for pickling tasks and their results.

# %%
# CpProcessPoolExecutor works! Also, all we had to do was change the
# plugin name to "cpprocess".
with ExecutorCtx("cpprocess", 4, mp_context=get_context("spawn")) as exe:
    futs = [exe.submit(sorting_test_with_big_random_list, seed) for seed in range(25)]
    for seed, fut in enumerate(futs):
        if not fut.result():
            print(f"Seed {seed} failed.")

# %% [markdown]
# We might also try a fancier one like `loky`'s. Their executor is
# a from-scratch rewrite of `ProcessPoolExecutor`, with built-in
# `cloudpickle` support, and various robustness improvements over the
# built-in one. It works fine too.

# %%
# loky's ProcessPoolExecutor also works. Similar to before, we only had to
# change the plugin name to "loky".
with ExecutorCtx("loky", 4, mp_context=get_context("spawn")) as exe:
    futs = [exe.submit(sorting_test_with_big_random_list, seed) for seed in range(25)]
    for seed, fut in enumerate(futs):
        if not fut.result():
            print(f"Seed {seed} failed.")

# %% [markdown]
# Now, we're done debugging. We know that we just need to make sure
# we use a `cloudpickle`-enabled plugin like `"cpprocess"` or `"loky"`.
