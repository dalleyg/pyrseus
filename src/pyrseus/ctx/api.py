"""
Defines the plugin API used by `~pyrseus.ctx.mgr.ExecutorCtx` to register
executor backends.

Each module in the `pyrseus.ctx.plugins` packages must define an ``ENTRY_POINT``
global variable that implements the `.ExecutorPluginEntryPoint` protocol. Third
party plugins must also supply an entry point object, but they are freer to
choose how to have them registered.
"""
from __future__ import annotations

from concurrent.futures import Executor
from enum import auto
from functools import cached_property
from inspect import Parameter, signature
from types import TracebackType
from typing import Callable, List, Optional, Protocol, Set, Tuple, Type, Union

try:
    from enum import StrEnum as _StrishEnum  # >= py 3.11
except ImportError:
    from enum import Enum as _StrishEnum  # < py 3.11

ExcInfo = Tuple[Optional[Type], Optional[BaseException], Optional[TracebackType]]
"""
Formal type for the return value of ``sys.exc_info()``.
"""

PreExitFunc = Callable[[Executor, ExcInfo], None]
"""
Formal type for a "pre exit" function that a plugin's ``ENTRY_POINT.create``
method can return. See `.ExecutorPluginEntryPoint.create` for details.
"""


class OnError(_StrishEnum):
    """
    Enum used by `.ExecutorPluginEntryPoint.create` to indicate how the user
    would like workers and pending futures to be handled when an uncaught
    exception is passing through the context's ``__exit__`` method.

    >>> OnError.get("kill")
    <OnError.KILL_WORKERS...>

    >>> OnError.get(OnError.KILL)
    <OnError.KILL_WORKERS...>
    """

    WAIT = auto()
    """
    Wait on all pending futures to enter a final state. This is the default
    behavior for most existing executors.
    """

    CANCEL_FUTURES = auto()
    """
    Cancel as many pending futures as possible. Note that for some executors
    like |loky|_'s pre-schedule futures, so many fewer futures than expected may
    actually be cancelled.
    """

    KILL_WORKERS = auto()
    """
    If the executor has a way of abruptly killing its workers, do so. Otherwise,
    this falls back to the `.CANCEL_FUTURES` behavior (e.g. for
    `~concurrent.futures.ThreadPoolExecutor`).

    .. note::

       This may skip `atexit` handlers or other normal worker cleanup. That
       said, this is frequently not a problem because:

        - executor-based applications typically need to be robust to abrupt
          worker failures anyway,

        - with this option, the whole worker pool is being shut down, and

        - the decoupling of workers from tasks in executor interfaces makes it
          harder to write the types of algorithms that require cross-process
          locks.
    """

    CANCEL = CANCEL_FUTURES
    """
    An alias for `.CANCEL_FUTURES`.
    """

    KILL = KILL_WORKERS
    """
    An alias for `.KILL_WORKERS`.
    """

    @classmethod
    def get(cls, arg: OnErrorLike) -> OnError:
        """
        Factory method that can take either a string or an instance of this
        class and return an instance to this class.
        """
        if isinstance(arg, str):
            return cls[arg.upper()]
        elif isinstance(arg, cls):
            return arg
        else:
            # Don't allow ints. This way we can safely use StrEnum in py 3.11+
            # without breaking users migrating from older versions of Python.
            raise TypeError(arg)


OnErrorLike = Union[OnError, str]
"""
The set of types accepted by `.OnError.get`.
"""


def extract_keywords(factory: Callable) -> Set[str]:
    """
    Helper for `.ExecutorPluginEntryPoint.allowed_keywords` properties. It
    inspects `.ExecutorPluginEntryPoint.create`-like functions an extracts their
    keyword argument names. This improves the maintainability of plugins where
    the factory publishes its accepted keywords in its call signature.

    :param factory: a function or method that creates an executor, takes at most
        one positional argument (other than possibly ``self``), and whose first
        (non-self) argument is called ``max_workers``.

    :return: the set of all keyword arguments the ``factory`` can accept, other
        than ``self`` and ``max_workers``. If ``factory`` is a class and its
        constructor takes a variadic ``**kwargs``-style argument, this function
        walks through its MRO superclasses to accumulate additional keywords,
        under the assumption that all subclass ``**kwargs`` get passed without
        name changes to the superclass.
    """
    kwarg_names = set()
    if isinstance(factory, type):
        funcs = factory.__mro__
    else:
        funcs = [factory]
    for func in funcs:
        if isinstance(func, type) and ("__init__" not in func.__dict__):
            # Skip mixins.
            continue
        saw_kwargs = False
        sig = signature(func)
        max_workers_idx = 0
        for i, (name, spec) in enumerate(sig.parameters.items()):
            if i == max_workers_idx:
                if spec.kind not in (
                    Parameter.POSITIONAL_ONLY,
                    Parameter.POSITIONAL_OR_KEYWORD,
                ):
                    raise TypeError(
                        f"The factory's first (non-self) parameter must "
                        f"be positional (and be for a max_workers argument).\n"
                        f"    factory:         {func}\n"
                        f"    parameter index: {i}\n"
                        f"    parameter name:  {name}\n"
                        f"    parameter kind:  {spec.kind}\n"
                    )
                if name == "self":
                    max_workers_idx = 1
                elif name == "max_workers":
                    pass
                else:
                    # For now, we'll be fussy about names for simplicity.
                    raise TypeError(
                        "The factory's first (non-self) parameter must "
                        "be named 'max_workers'."
                    )
            elif spec.kind == Parameter.POSITIONAL_ONLY:
                raise TypeError(
                    "The only allowed positional-only (non-self) parameter for "
                    "a factory is 'max_workers'."
                )
            elif spec.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY):
                if spec.default is Parameter.empty:
                    raise TypeError(f"Parameter {name!r} has no default.")
                kwarg_names.add(name)
            elif spec.kind is Parameter.VAR_POSITIONAL:
                raise TypeError(f"Factories must not take *args-style varargs: {func}")
            elif spec.kind is Parameter.VAR_KEYWORD:
                # We've seen variadic kwargs, so if we're chasing the mro graph,
                # we need to keep going.
                saw_kwargs = True
            else:
                raise TypeError(f"Unknown factory parameter kind: {spec.kind}")
        if not saw_kwargs:
            break
    return kwarg_names


class ExecutorPluginEntryPoint(Protocol):
    """
    Each plugin module must have an ``ENTRY_POINT`` attribute that follows the
    protocol defined by this class. That variable tells the
    `pyrseus.ctx.mgr.ExecutorCtx` plugin system how to use the module's
    executor.
    """

    @property
    def supports_serial(self) -> bool:
        """
        Whether this plugin's ``ENTRY_POINT.create`` function can be called with
        ``max_workers`` = 0.
        """
        raise NotImplementedError

    @property
    def supports_concurrent(self) -> bool:
        """
        Whether this plugin's ``ENTRY_POINT.create`` function can be called with
        ``max_workers`` > 0.
        """
        raise NotImplementedError

    @property
    def is_available(self) -> bool:
        """
        Whether this plugin should be enabled. Typically this property will do
        an import check on the underlying executor module using
        `~pyrseus.core.sys.module_exists`.

        If this is false, `~pyrseus.ctx.mgr.ExecutorCtx` will never invoke the
        `.create` method for this plugin.
        """
        raise NotImplementedError

    @cached_property
    def allowed_keywords(self) -> Union[Tuple[str], List[str], Set[str]]:
        """
        The complete list of keywords that this plugin's ``ENTRY_POINT.create``
        method supports.

        If the user requests a `~pyrseus.ctx.mgr.ExecutorCtx` for this plugin,
        but they included some keywords that are missing from this set:

        - any keywords there *are* in any other available plugin's
          ``allowed_keywords`` will be silently dropped, and

        - any other keywords will trigger an exception.

        This filtering and validation is done by
        `~pyrseus.ctx.registry.filter_kwargs`.

        See the various plugins in the `pyrseus.ctx.plugins` package for
        examples of how to customize this property, especially when
        ``ENTRY_POINT.create`` takes a variadic ``**kwargs``-style argument.
        """
        return extract_keywords(self.create)

    def create(
        self,
        max_workers: Optional[int] = None,
        *,
        on_error: OnErrorLike = OnError.KILL_WORKERS,
        **kwargs,
    ) -> Tuple[Executor, Optional[PreExitFunc]]:
        """
        Creates an executor instance, returning both the instance and an
        optional function that will be called just before that executor's
        ``__exit__`` method is called at context exit time.

        To see the full list of keyword arguments that this plugin supports,
        use `~pyrseus.ctx.registry.get_keywords_for_plugin`.

        :param max_workers: the maximum number of workers the user has
            requested. ``ENTRY_POINT.create`` must support this as a positional
            argument, even for serial-only plugins that ignore its value.

        :param on_error: controls what `pyrseus.ctx.mgr.ExecutorCtx.__exit__`
            should do if an exception is passing through it. See `.OnError` for
            details.

        :param kwargs: additional arguments passed to the executor's
            constructor. These are filtered by
            `~pyrseus.ctx.registry.filter_kwargs`, using this plugin's
            ``ENTRY_POINT.allowed_keywords`` property.
        """
        raise NotImplementedError
