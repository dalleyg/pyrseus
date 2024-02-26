from typing import Optional, Union

from .registry import filter_kwargs, get_entry_point

__all__ = ["ExecutorCtx"]


class ExecutorCtx:
    """
    Context manager that acts as a factory for many types of serial and
    concurrent `~.concurrent.futures.Executor` classes, where switching between
    executor plugins requires minimal user effort.

    Choosing the Plugin and Max Workers
    ===================================

    `.ExecutorCtx` accepts up to two positional arguments. They are used to
    choose which :ref:`plugin <plugins>` (and thus underlying executor) will be
    used and what its ``max_workers`` argument will be. All other constructor
    arguments will be passed to the plugin.

    No Positional Arguments
    -----------------------

    With no positional arguments, `.ExecutorCtx` will default to creating a
    multiprocess executor, with one worker per available CPU core, respecting
    the current CPU affinity mask (see
    `~pyrseus.core.sys.get_num_available_cores`).

        >>> with ExecutorCtx() as exe:
        ...    print(exe.submit(sum, [1, 2, 3, 4]).result())
        10

    One Positional Integer Argument
    -------------------------------

    If one positional integer argument is supplied, `.ExecutorCtx` will either
    use a special serial executor (if 0):

        >>> import os
        >>> with ExecutorCtx(0) as exe:
        ...     assert exe.submit(os.getpid).result() == os.getpid()  # serial

    or its default concurrent executor (if positive):

        >>> with ExecutorCtx(1) as exe:
        ...     assert exe.submit(os.getpid).result() != os.getpid()  # concurrent

    One Positional String Argument
    ------------------------------

    If one positional string argument is supplied, it names a specific
    `.ExecutorCtx` plugin that should be used. That plugin's default
    ``max_workers`` will be used.

        >>> with ExecutorCtx("inline") as exe:
        ...     # "inline" is a serial plugin that immediately runs tasks
        ...     # in the same process and thread.
        ...     assert exe.submit(os.getpid).result() == os.getpid()

    Two Positional Arguments
    ------------------------

    If two positional arguments are supplied, the first must be the plugin name,
    and the second the ``max_workers`` to use with it:

        >>> with ExecutorCtx("process", 1) as exe:
        ...     futs = [exe.submit(os.getpid) for _ in range(10)]
        ...     some_worker_pids = {fut.result() for fut in futs}
        ...     assert len(some_worker_pids) == 1
        ...     assert all(
        ...         worker_pid != os.getpid()
        ...         for worker_pid in some_worker_pids
        ...     )

    Extra Arguments
    ===============

    After the plugin and/or ``max_workers`` arguments, all others are
    keyword-only. See `~pyrseus.ctx.registry.get_all_allowed_keywords`,
    `~pyrseus.ctx.registry.get_keywords_by_plugin`, and
    `~pyrseus.ctx.registry.get_keywords_for_plugin` for the full list of
    supported keywords based on which plugins are installed on your system.

    Irrelevant Keywords are Auto-ignored
    ------------------------------------

    Keyword arguments that are not supported by the selected plugin are silently
    ignored. This makes it easier for users to switch between executor types by
    just changing the positional arguments and not needing to update all of the
    keyword arguments.

    For example, suppose you're troubleshooting a pickling problem with the
    `~pyrseus.ctx.plugins.mpi4py` plugin, where you've also added several
    plugin-specific keyword arguments to the `.ExecutorCtx` constructor call:

        >>> # Uh oh, we have a pickling problem!
        >>> from pyrseus.ctx.registry import skip_if_unavailable
        >>> skip_if_unavailable("mpi4py")
        >>> with ExecutorCtx(
        ...     "mpi4py",
        ...     main=True, path=['some/lib', 'another/lib'] # (cp)mpi4py-specific kwargs
        ... ) as exe:
        ...     print(exe.submit(lambda: 42).result())
        Traceback (most recent call last):
        ...
        _pickle.PicklingError: Can't pickle ...

    A good way to troubleshoot these types of issues is with the
    `~pyrseus.ctx.plugins.pinline` and/or `~pyrseus.ctx.plugins.cpinline`
    plugins. We can switch to either of those by just changing the first
    positional argument and leaving the other |mpi4py|_-specific arguments
    there. In this case, we see that the problem is that we need a
    |cloudpickle|_-enabled plugin like `~pyrseus.ctx.plugins.cpmpi4py` instead
    of the base `~pyrseus.ctx.plugins.mpi4py`.

        >>> # We can reproduce it with the pinline plugin. If we have a debugger
        >>> # like pdb or ipdb, we could trace into the code since this plugin
        >>> # runs its tasks immediately and in the same process and thread
        >>> # (debugging not shown here).
        >>> #
        >>> # NOTE: the pinline plugin doesn't take `main` and `path` arguments,
        >>> # but ExecutorCtx knows this so it suppresses them.
        >>> skip_if_unavailable("pinline")
        >>> with ExecutorCtx(
        ...     "pinline",
        ...     main=True, path=['some/lib', 'another/lib'] # auto-ignored
        ... ) as exe:
        ...     print(exe.submit(lambda: 42).result())
        Traceback (most recent call last):
        ...
        _pickle.PicklingError: Can't pickle ...

        >>> # Suppose we realize that we should be using a cloudpickle-enabled
        >>> # plugin. We try cpinline and see that it works.
        >>> skip_if_unavailable("cpinline")
        >>> with ExecutorCtx(
        ...     "cpinline",
        ...     main=True, path=['some/lib', 'another/lib'] # auto-ignored
        ... ) as exe:
        ...     print(exe.submit(lambda: 42).result())
        42

        >>> # Then we switch to cpmpi4py and see that it works too.
        >>> skip_if_unavailable("cpmpi4py")
        >>> with ExecutorCtx(
        ...     "cpmpi4py",
        ...     main=True, path=['some/lib', 'another/lib'] # (cp)mpi4py-specific kwargs
        ... ) as exe:
        ...     print(exe.submit(lambda: 42).result())
        42

    Invalid Keywords Give an Exception
    ----------------------------------

    Although `.ExecutorCtx` ignores keywords that are irrelevant for the
    currently selected plugin, it does check for keywords that aren't relevant
    for any plugins. This helps catch user mistakes.

        >>> with ExecutorCtx(this_is_not_a_valid_keyword="for_any_plugin"):
        ...     pass
        Traceback (most recent call last):
        ...
        TypeError: Keyword 'this_is_not_a_valid_keyword' is not accepted by any of the registered plugins:
            plugins: [...'inline'...]
            all allowed keywords: [...]

    Changing the Default Plugins
    ============================

    Pyrseus' default plugins are `~pyrseus.ctx.plugins.inline` when
    ``max_workers=0`` and `~pyrseus.ctx.plugins.process` otherwise. To change
    either of these defaults, use
    `~pyrseus.ctx.registry.SetDefaultExecutorPluginCtx` or
    `~pyrseus.ctx.registry.set_default_executor_plugin`.

    Creating New Plugins
    ====================

    See :ref:`writingplugins`.
    """

    def __init__(
        self,
        plugin_or_max_workers: Union[str, int, None] = None,
        /,
        max_workers: Optional[int] = None,
        **kwargs,
    ):
        plugin_name, max_workers, entry_point = get_entry_point(
            plugin_or_max_workers, max_workers
        )
        self._relevant_kwargs = filter_kwargs(entry_point, kwargs)

        # Record additional metadata for troubleshooting.
        self._plugin_name = plugin_name
        self._max_workers = max_workers
        self._entry_point = entry_point
        self._full_kwargs = kwargs

        # Invoke the factory and save the results.
        self._exe, self._pre_exit = entry_point.create(
            self._max_workers, **self._relevant_kwargs
        )

    def __enter__(self):
        """
        Enters the context of the underlying `~concurrent.futures.Executor`.
        """
        return self._exe.__enter__()

    def __exit__(self, *exc_info):
        if self._pre_exit is not None:
            self._pre_exit(self._exe, exc_info)
        return self._exe.__exit__(*exc_info)
