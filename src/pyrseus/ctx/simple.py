"""
A collection of helpers for writing `~pyrseus.ctx.api.ExecutorPluginEntryPoint`
subclasses that work with simple standard and semi-standard real-world
executors.
"""

import importlib
from concurrent.futures import ALL_COMPLETED, Executor, Future, wait
from functools import cached_property
from typing import Callable, Optional, Tuple, TypeVar

from pyrseus.core.sys import get_num_available_cores, module_exists
from pyrseus.ctx.api import (
    ExcInfo,
    ExecutorPluginEntryPoint,
    OnError,
    OnErrorLike,
    PreExitFunc,
    extract_keywords,
)

Ret = TypeVar("Ret")
"""
Represents the generic return type of a submitted callable.
"""


class OnExitProxy:
    """
    An `~concurrent.futures.Executor` proxy class that (a) tracks futures
    submitted to the specific executor, and (b) replaces the executor's
    ``__exit__`` method with a customized one from this class.

    - *When to Use:* This proxy is primarily useful for executors like |loky|_'s
      reusable ones. Without this wrapper, their context managers can
      unnecessarily wait on futures submitted by other managers or even
      submitted outside the context block. See the reusable variant of the
      `~pyrseus.ctx.plugins.loky` plugin for an example of using this proxy.

    - *When Not:* If you are using a normal executor where its context manager
      is the sole owner of its worker pool, considering using the lighter-weight
      `.shutdown_wait_on_error` and related functions instead of this proxy
      class.

    In the following example, ``e1_overblocks`` blocks until both its futures
    *and* ``e0``'s futures enter a final state:

        >>> from pyrseus.ctx.registry import skip_if_unavailable
        >>> import time

        >>> skip_if_unavailable("loky")
        >>> from loky import get_reusable_executor
        >>> t0 = time.time()
        >>> with get_reusable_executor() as e0:
        ...     _ = e0.submit(time.sleep, 1.0)
        ...     with get_reusable_executor() as e1_overblocks:
        ...         _ = e1_overblocks.submit(time.sleep, 0.1)
        ...     # e1_overblocks waited on e0's future too!
        ...     assert 1.0 <= time.time() - t0

    But with this wrapper, ``e1_precise`` only blocks on its own futures:

        >>> skip_if_unavailable("loky")
        >>> from loky import get_reusable_executor
        >>> with get_reusable_executor() as e0:
        ...     t0 = time.time()
        ...     _ = e0.submit(time.sleep, 1.0)
        ...     # Wrap the underlying executor in this proxy.
        ...     with OnExitProxy(
        ...         get_reusable_executor(),
        ...         on_error='cancel',
        ...         shutdown=False,
        ...     ) as e1_precise:
        ...         _ = e1_precise.submit(time.sleep, 0.1)
        ...     # e1_precise only blocked on its own future
        ...     assert 0.1 <= time.time() - t0 <= 0.9
        >>> # and e0 blocking still works as it should
        >>> assert 1.0 <= time.time() - t0

    :param exe: the executor to wrap. We forward all public attribute and method
        lookups to this object, not including special methods. This proxy is
        only designed for executors with trivial ``return self`` ``__enter__``
        methods.

    :param on_error: controls how ``exe`` and futures submitted via our
        `.submit` method are handled if an uncaught exception is passing through
        our `.__exit__` method. See `.OnError` for details. If ``shutdown`` is
        false, our `.__exit__` method uses `.wait` whenever possible instead of
        ``exe.shutdown(...)``.

    :param shutdown: whether to always call ``exe.shutdown(...)`` in our
        `.__exit__` method vs. try to use `.wait` instead when possible.
    """

    def __init__(self, exe: Executor, *, on_error: OnErrorLike, shutdown: bool):
        self._exe = exe
        self._on_error = OnError.get(on_error)
        self._shutdown = shutdown
        self._futs = set()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if self._shutdown:
            if exc_info == (None, None, None):
                self._exe.shutdown(wait=True)
            elif self._on_error is OnError.WAIT:
                self._exe.shutdown(wait=True)
            elif self._on_error is OnError.CANCEL_FUTURES:
                self._exe.shutdown(wait=True, cancel_futures=True)
            elif self._on_error is OnError.KILL_WORKERS:
                self._exe.shutdown(wait=True, kill_workers=True)
            else:
                raise ValueError(self._on_error)
        else:
            if exc_info == (None, None, None):
                wait(self._futs, return_when=ALL_COMPLETED)
            elif self._on_error is OnError.WAIT:
                wait(self._futs, return_when=ALL_COMPLETED)
            elif self._on_error is OnError.CANCEL_FUTURES:
                self.cancel_all()
                wait(self._futs, return_when=ALL_COMPLETED)
            elif self._on_error is OnError.KILL_WORKERS:
                # For now, we'll ask the executor to shut down everything, since
                # no major real-world executors publicly support killing just
                # the workers associated with specific tasks.
                self._exe.shutdown(wait=True, kill_workers=True)
            else:
                raise ValueError(self._on_error)

    def submit(self, fcn: Callable[..., Ret], /, *args, **kwargs) -> Future[Ret]:
        """
        Submits a task to the underlying executor, saving the future so that we
        can automatically block on it when the context is exited.
        """
        # Submit it to the underlying executor.
        fut = self._exe.submit(fcn, *args, **kwargs)
        # Save the future so that we can wait on it in __exit__ if needed.
        self._futs.add(fut)
        # Be nice and trim the set of tracked futures as fast as we can. This
        # way we don't leak memory if the results are large. This must come
        # after self._futs.add(fut) because the callback runs immediately if
        # already completed.
        fut.add_done_callback(lambda fut: self._futs.remove(fut))
        return fut

    def cancel_all(self) -> Tuple[int, int]:
        """
        Cancels all futures submitted through this executor proxy, but not any
        submitted to the underlying executor through other means, e.g. an
        aliased reference to the same |loky|_ reusable executor.

        :return: a tuple containing the number of futures that this method
            attempted to cancel, and the number it successfully cancelled.
            Futures that are already in a running or later state cannot be
            cancelled.
        """
        num_cancelled = 0
        futs = tuple(self._futs)  # make this race-free w.r.t. done callbacks
        for fut in futs:
            num_cancelled += fut.cancel()
        return len(futs), num_cancelled

    def __getattr__(self, name):
        """
        Makes this a perfect proxy class for public methods and attributes that
        aren't overridden in this proxy.
        """
        # For now, we'll only proxy public members, not special or non-public
        # ones. External users shouldn't be directly using non-public members.
        # Extra care has to be taken with some special members, so we don't even
        # try proxying those.
        if name.startswith("_"):
            if name not in dir(self._exe):
                raise AttributeError(name)
            else:
                raise AttributeError(f"Only public members are proxied, not: {name}")
        return getattr(self._exe, name)


def shutdown_wait_on_error(ctx: Executor, exc_info: ExcInfo):
    """
    Simple callback helper that invokes ``ctx.shutdown(wait=True)`` if
    ``exc_info`` is all ``None``.

    - *When to Use:* When making a plugin using `.SimpleEntryPoint`, consider
      having your `~.SimpleEntryPoint.wrap_for_wait_on_error` return this
      function as the second element of its returned tuple (the `.PreExitFunc`
      part). It will typically provide the desired behavior if ``ctx`` is the
      sole owner of its worker pool. See the `~pyrseus.ctx.plugins.process`
      plugin for an example.

    - *When Not:* For worker pools that may be shared between contexts, consider
      using `.OnExitProxy` to wrap the first returned element instead.
    """
    if exc_info != (None, None, None):
        ctx.shutdown(wait=True)


def shutdown_cancel_on_error(ctx: Executor, exc_info: ExcInfo):
    """
    Like `.shutdown_wait_on_error`, but invokes ``ctx.shutdown(wait=True,
    cancel_futures=True)``, and is instead designed for use in
    `~.SimpleEntryPoint.wrap_for_cancel_on_error` overrides.

    Only use this callback if your executor class accepts a ``cancel_futures``
    argument to its ``shutdown`` method.
    """
    if exc_info != (None, None, None):
        ctx.shutdown(wait=True, cancel_futures=True)


def shutdown_kill_on_error(ctx: Executor, exc_info: ExcInfo):
    """
    Like `.shutdown_wait_on_error`, but invokes ``ctx.shutdown(wait=True,
    kill_workers=True)``, and is instead designed for use in
    `~.SimpleEntryPoint.wrap_for_kill_on_error` overrides.

    Only use this callback if your executor class accepts a ``kill_workers``
    argument to its ``shutdown`` method.
    """
    if exc_info != (None, None, None):
        ctx.shutdown(wait=True, kill_workers=True)


class SimpleEntryPoint(ExecutorPluginEntryPoint):
    """
    Abstract subclass of `.ExecutorPluginEntryPoint` that works when the
    executor class' constructor is already friendly the default
    ``allowed_keywords`` property's implementation. Plugin authors simply need
    to override the following properties (can be done with simple attributes):

    - `.supports_serial`
    - `.supports_concurrent`
    - `.executor_module_name`
    - `.executor_class_name`
    - `.wrap_for_wait_on_error` (unless `.create` is overridden)
    - `.wrap_for_cancel_on_error` (unless `.create` is overridden)
    - `.wrap_for_kill_on_error` (unless `.create` is overridden)
    """

    @property
    def executor_module_name(self) -> str:
        """
        Fully qualified name of the module that hosts the executor class
        provided by this entry point. This class will lazily import the module
        so that `.is_available` checks can be made safely.

        This must be overridden for all subclasses.
        """
        raise NotImplementedError

    @property
    def executor_class_name(self) -> str:
        """
        Name of the executor class that is provided by this entry point. This
        should be just the class name itself, not including its module.

        This must be overridden for all subclasses.
        """
        raise NotImplementedError

    @property
    def extra_modules_required(self) -> Optional[Tuple[str]]:
        """
        If supplied, the default `.is_available` property will also check that
        these modules can be found by the importer.

        This must be overridden for all subclasses.
        """
        return ()

    @cached_property
    def is_available(self) -> bool:
        if extras := self.extra_modules_required:
            if isinstance(extras, str):
                # We'll let it slide if it looks like it's just a single module
                # name, but let's not start splitting strings here.
                if ("," in extras) or (":" in extras):
                    raise TypeError(
                        f"{self.extra_modules_required=!r} should "
                        f"be a tuple of strings."
                    )
                extras = (extras,)
            if not all(module_exists(name) for name in extras):
                return False
        return module_exists(self.executor_module_name)

    @cached_property
    def ExecutorCls(self) -> type:
        """
        Returns the lazily-imported executor class to use for this plugin. This
        uses the `.executor_module_name` and `.executor_class_name` attributes.

        If your plugin needs to use multiple classes or wrappers, consider
        directly subclassing `.ExecutorPluginEntryPoint` instead of using
        `.SimpleEntryPoint`.
        """
        if not self.is_available:
            # Safety check, in case the concrete subclass has a custom
            # is_available property.
            raise RuntimeError(
                "This entry point's module is unavailable and/or it has "
                "otherwise been disabled."
            )
        mod = importlib.import_module(self.executor_module_name)
        return getattr(mod, self.executor_class_name)

    @cached_property
    def allowed_keywords(self) -> Tuple[str]:
        return extract_keywords(self.ExecutorCls) | extract_keywords(type(self).create)

    @cached_property
    def default_max_workers(self):
        """
        When given ``max_workers=None``, `.create` will use this value as the
        maximum number of workers. Only applicable if `.supports_concurrent` is
        true. This base implementation uses the number of visible physical cores
        that are not blocked by the current CPU affinity mask. See
        `~pyrseus.core.sys.get_num_available_cores` for details.
        """
        return max(1, get_num_available_cores())

    def wrap_for_wait_on_error(self, ctx) -> Tuple[Executor, Optional[PreExitFunc]]:
        """
        Called by `.create` when its ``on_error`` argument is `.OnError.WAIT`,
        to ensure the returned context manager provides those semantics. Returns
        the same tuple type as `.ExecutorPluginEntryPoint.create`.

        This must be overridden for all subclasses, unless `.create` is
        overridden.
        """
        raise NotImplementedError

    def wrap_for_cancel_on_error(self, ctx) -> Tuple[Executor, Optional[PreExitFunc]]:
        """
        Called by `.create` when its ``on_error`` argument is
        `.OnError.CANCEL_FUTURES`, to ensure the returned context manager
        provides those semantics. Returns the same tuple type as
        `.ExecutorPluginEntryPoint.create`.

        This must be overridden for all subclasses, unless `.create` is
        overridden.
        """
        raise NotImplementedError

    def wrap_for_kill_on_error(self, ctx) -> Tuple[Executor, Optional[PreExitFunc]]:
        """
        Called by `.create` when its ``on_error`` argument is
        `.OnError.KILL_WORKERS`, to ensure the returned context manager provides
        those semantics. Returns the same tuple type as
        `.ExecutorPluginEntryPoint.create`.

        This must be overridden for all subclasses, unless `.create` is
        overridden.
        """
        raise NotImplementedError

    def create(
        self,
        max_workers: Optional[int] = None,
        *,
        on_error: OnErrorLike = OnError.KILL,
        **kwargs,
    ):
        """
        Creates an executor instance using this plugin.

        :param max_workers: the maximum number of workers to use. If `None`,
            this plugin will use `.default_max_workers` if it's a concurrent
            plugin.

        :param on_error: controls what `pyrseus.ctx.mgr.ExecutorCtx.__exit__`
            should do if an exception is passing through it. A pre-exit callback
            will be created if needed to provide the requested behavior. See
            `.get_standard_pre_exit_handler` for details.
        """
        if self.supports_concurrent and (max_workers is None):
            max_workers = self.default_max_workers
        on_error = OnError.get(on_error)

        ctx = self.ExecutorCls(max_workers, **kwargs)

        if on_error is OnError.WAIT:
            return self.wrap_for_wait_on_error(ctx)
        elif on_error is OnError.CANCEL_FUTURES:
            return self.wrap_for_cancel_on_error(ctx)
        elif on_error is OnError.KILL_WORKERS:
            return self.wrap_for_kill_on_error(ctx)
        else:
            raise ValueError(on_error)
