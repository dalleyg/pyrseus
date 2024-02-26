"""
Manages `~pyrseus.ctx.mgr.ExecutorCtx`'s plugin registry.

`~pyrseus.ctx.mgr.ExecutorCtx` is a factory class that can create many different
types of `~concurrent.futures.Executor`s. This module provides the mechanism it
uses to track all of those different types, via a registry of named
`pyrseus.ctx.api.ExecutorPluginEntryPoint` objects.

See :ref:`writingplugins` for more information on how to create a plugin to wrap
your own executor classes.

Related Modules and Packages
----------------------------

- `pyrseus.executors`: a package providing some but not all of the underlying
  executors that are made available to `~pyrseus.ctx.mgr.ExecutorCtx` via this
  registry. Users are free to directly use those executor classes if they do not
  wish to use `~pyrseus.ctx.mgr.ExecutorCtx`.

- `pyrseus.ctx.api`: provides the base class
  `~pyrseus.ctx.api.ExecutorPluginEntryPoint`. This module maintains a registry
  of concrete instances of that class.

- `pyrseus.ctx.mgr`: provides `~pyrseus.ctx.mgr.ExecutorCtx`, the primary client
  of this module.

- `pyrseus.ctx.plugins`: a package of plugin modules for
  `~pyrseus.ctx.mgr.ExecutorCtx`. Each module in that package must have a global
  variable called ``ENTRY_POINT`` that is an
  `~pyrseus.ctx.api.ExecutorPluginEntryPoint` instance. The plugin's name will
  be the same as the module's name, after ``"pyrseus.ctx.plugins."``. This
  registry auto-discovers all such entry points at import time.

- `pyrseus.ctx.simple`: provides a simpler alternative API to the base one
  provided by `pyrseus.ctx.api`. Plugin authors can use whichever API suits
  their needs better.

Handling Keywords
-----------------

The following functions are related to validating and filtering keyword
arguments so that it can both (a) silently ignore irrelevant keywords for a
particular plugin, yet (b) still provide useful errors for keywords that aren't
valid for any available plugin:

- `filter_kwargs`
- `get_all_allowed_keywords`
- `get_keywords_by_plugin`
- `get_keywords_for_plugin`

Test Helpers
------------

The following functions are primarily useful for writing tests that only work if
a particular plugin is installed:

- `is_plugin_available`
- `skip_if_unavailable`

Registry Management
-------------------

The following are useful for managing the registry itself.

- `get_entry_point`: retrieves the requested plugin's
  `~pyrseus.ctx.api.ExecutorPluginEntryPoint` object, if possible. This is what
  `~pyrseus.ctx.mgr.ExecutorCtx` uses.

- `register_plugin`: registers a new or replacement plugin. This is called
  automatically for each module in the `pyrseus.ctx.plugins` package when this
  registry module is imported. It can also be directly called for 3rd party
  plugins.

- `SetDefaultExecutorPluginCtx`: temporarily changes the default concurrent
  and/or serial plugin, with standard context manager semantics.

- `set_default_executor_plugin`: permanently changes the default concurrent
  and/or serial plugin.
"""

import importlib
import pkgutil
from contextlib import contextmanager
from typing import Any, Dict, Optional, Set, Tuple, Union

from .api import ExecutorPluginEntryPoint

__ALL__ = [
    "filter_kwargs",
    "get_all_allowed_keywords",
    "get_entry_point",
    "get_keywords_by_plugin",
    "get_keywords_for_plugin",
    "is_plugin_available",
    "register_plugin",
    "set_default_executor_plugin",
    "SetDefaultExecutorPluginCtx",
    "skip_if_unavailable",
]

_SERIAL_ENTRY_POINTS: Dict[str, ExecutorPluginEntryPoint] = {}
_CONCURRENT_ENTRY_POINTS: Dict[str, ExecutorPluginEntryPoint] = {}
_KNOWN_BUT_UNAVAILABLE_ENTRY_POINTS: Dict[str, ExecutorPluginEntryPoint] = {}
_DEFAULT_SERIAL_ENTRY_POINT_NAME: str = "inline"
_DEFAULT_CONCURRENT_ENTRY_POINT_NAME: str = "cpprocess"
_ALL_AVAILABLE_KEYWORDS: Set[str] = set()


def get_all_allowed_keywords():
    """
    Returns the full set of allowed keyword arguments to
    `~pyrseus.ctx.mgr.ExecutorCtx`. This is the union of the allowed keyword
    arguments across all registered plugins.

    >>> get_all_allowed_keywords()
    {...'mp_context'...}
    """
    return _ALL_AVAILABLE_KEYWORDS


def get_keywords_for_plugin(
    plugin_or_max_workers: Optional[Union[str, int]] = None,
    /,
    max_workers: Optional[int] = None,
):
    """
    Returns the full set of keyword arguments that will be forwarded to a
    particular plugin.

    >>> get_keywords_for_plugin("process")
    {...'mp_context'...}

    >>> assert get_keywords_for_plugin("process") <= get_all_allowed_keywords()
    """
    plugin, max_workers = _interpret_plugin_and_max_workers(
        plugin_or_max_workers, max_workers
    )
    _, _, entry_point = get_entry_point(plugin, max_workers)
    return entry_point.allowed_keywords


def get_keywords_by_plugin():
    """
    Returns the full set of keyword arguments, on a per-plugin basis.

    >>> by_plugin = get_keywords_by_plugin()
    >>> sorted(by_plugin['inline'])
    []
    >>> sorted(by_plugin['process'])
    [...'initializer'...'mp_context'...]

    """
    ret = {}
    for mapping in (_CONCURRENT_ENTRY_POINTS, _SERIAL_ENTRY_POINTS):
        for plugin, entry_point in mapping.items():
            accum = ret.setdefault(plugin, set())
            accum |= entry_point.allowed_keywords
    return ret


def register_plugin(name: str, entry_point: ExecutorPluginEntryPoint) -> bool:
    """
    Registers a new `.ExecutorPluginEntryPoint` as a plugin associated with the
    given ``name``.

    Registering an unavailable plugin is fine: this module will respect
    `entry_point.is_available <.ExecutorPluginEntryPoint.is_available>` whenever
    it uses the entry point object.
    """
    if not entry_point.is_available:
        # Silently ignore any plugins that don't have their requirements met or
        # have otherwise been disabled.
        _KNOWN_BUT_UNAVAILABLE_ENTRY_POINTS[name] = entry_point
    else:
        # Extract and validate the metadata before mutating and global data
        # structures.
        allowed_keywords = entry_point.allowed_keywords
        supports_serial = entry_point.supports_serial
        supports_concurrent = entry_point.supports_concurrent
        if not isinstance(allowed_keywords, (tuple, list, set)):
            raise TypeError(f"{entry_point.allowed_keywords=} must be a set-like")
        if not isinstance(supports_serial, bool):
            raise TypeError(f"{entry_point.supports_serial=} must be a bool")
        if not isinstance(supports_concurrent, bool):
            raise TypeError(f"{entry_point.supports_concurrent=} must be a bool")
        if not supports_serial and not supports_concurrent:
            raise RuntimeError(
                f"The plugin for {name} must support at least one mode "
                f"(serial and/or concurrent)."
            )
        # Perform the registration.
        _ALL_AVAILABLE_KEYWORDS.update(allowed_keywords)
        _KNOWN_BUT_UNAVAILABLE_ENTRY_POINTS.pop(name, None)
        if supports_serial:
            _SERIAL_ENTRY_POINTS[name] = entry_point
        if supports_concurrent:
            _CONCURRENT_ENTRY_POINTS[name] = entry_point


def set_default_executor_plugin(
    name: str, serial: Optional[bool] = None, concurrent: Optional[bool] = None
) -> Tuple[str, str]:
    """
    Permanently sets the default serial and/or concurrent plugin for the current
    process. This is primarily intended for:

    - being the backend for `.SetDefaultExecutorPluginCtx`,
    - setting the defaults for entire interactive Jupyter notebook and IPython
      sessions,
    - setting the defaults for entire scripts, and
    - configuring user- and site-wide defaults.
    """
    global _DEFAULT_SERIAL_ENTRY_POINT_NAME
    global _DEFAULT_CONCURRENT_ENTRY_POINT_NAME
    old_serial = _DEFAULT_SERIAL_ENTRY_POINT_NAME
    old_concurrent = _DEFAULT_CONCURRENT_ENTRY_POINT_NAME

    if serial is None:
        if name in _SERIAL_ENTRY_POINTS:
            _DEFAULT_SERIAL_ENTRY_POINT_NAME = name
    elif serial:
        if name not in _SERIAL_ENTRY_POINTS:
            raise ValueError(f"{name} is a concurrent-only plugin")
        _DEFAULT_SERIAL_ENTRY_POINT_NAME = name

    if concurrent is None:
        if name in _CONCURRENT_ENTRY_POINTS:
            _DEFAULT_CONCURRENT_ENTRY_POINT_NAME = name
    elif concurrent:
        if name not in _CONCURRENT_ENTRY_POINTS:
            raise ValueError(f"{name} is a serial-only plugin")
        _DEFAULT_CONCURRENT_ENTRY_POINT_NAME = name

    return old_serial, old_concurrent


@contextmanager
def SetDefaultExecutorPluginCtx(
    name: str, serial: Optional[bool] = None, concurrent: Optional[bool] = None
):
    """
    Temporarily sets the default serial and/or concurrent plugin.

    >>> print("Default serial entry point:    ", get_entry_point(0)[0])
    Default serial entry point:     inline
    >>> print("Default concurrent entry point:", get_entry_point(1)[0])
    Default concurrent entry point: cpprocess

    >>> skip_if_unavailable("cpinline")
    >>> with SetDefaultExecutorPluginCtx("cpinline"):
    ...     print("Default serial entry point:    ", get_entry_point(0)[0])
    ...     print("Default concurrent entry point:", get_entry_point(1)[0])
    Default serial entry point:     cpinline
    Default concurrent entry point: cpprocess

    >>> with SetDefaultExecutorPluginCtx("process"):
    ...     print("Default serial entry point:    ", get_entry_point(0)[0])
    ...     print("Default concurrent entry point:", get_entry_point(1)[0])
    Default serial entry point:     inline
    Default concurrent entry point: process

    This function guards against mixing up concurrent vs. serial plugins.

    >>> skip_if_unavailable("cpinline")
    >>> with SetDefaultExecutorPluginCtx("cpinline", concurrent=True):
    ...     pass
    Traceback (most recent call last):
    ...
    ValueError: cpinline is a serial-only plugin
    """
    global _DEFAULT_SERIAL_ENTRY_POINT_NAME
    global _DEFAULT_CONCURRENT_ENTRY_POINT_NAME
    old_serial, old_concurrent = set_default_executor_plugin(name, serial, concurrent)
    try:
        yield old_serial, old_concurrent
    finally:
        _DEFAULT_SERIAL_ENTRY_POINT_NAME = old_serial
        _DEFAULT_CONCURRENT_ENTRY_POINT_NAME = old_concurrent


def get_entry_point(
    plugin_or_max_workers: Optional[Union[str, int]] = None,
    /,
    max_workers: Optional[int] = None,
) -> Tuple[str, Optional[int], ExecutorPluginEntryPoint]:
    """
    Returns the resolved plugin ``name``, (potentially resolved)
    ``max_workers``, and `.ExecutorPluginEntryPoint` object associated with the
    requested plugin.

    >>> get_entry_point("inline")[2].allowed_keywords
    set()

    >>> get_entry_point("not-a-plugin")
    Traceback (most recent call last):
    ...
    ValueError: Unknown ExecutorCtx plugin name: 'not-a-plugin'

    :param name: the name of the desired plugin. If `None`, then ``max_workers``
        will be used to auto-choose between the default serial and default
        concurrent plugin.

    :param max_workers: used as needed to help find the right plugin.
    """
    name, max_workers = _interpret_plugin_and_max_workers(
        plugin_or_max_workers, max_workers
    )
    if max_workers is None:
        if name is None:
            name = _DEFAULT_CONCURRENT_ENTRY_POINT_NAME
            entry_point = _CONCURRENT_ENTRY_POINTS[name]
        elif name in _CONCURRENT_ENTRY_POINTS:
            entry_point = _CONCURRENT_ENTRY_POINTS[name]
        elif name in _SERIAL_ENTRY_POINTS:
            entry_point = _SERIAL_ENTRY_POINTS[name]
        elif name in _KNOWN_BUT_UNAVAILABLE_ENTRY_POINTS:
            raise ValueError(
                f"There is a {name!r} ExecutorCtx plugin, but the plugin "
                f"has marked itself as unavailable in this process."
            )
        else:
            raise ValueError(f"Unknown ExecutorCtx plugin name: {name!r}")
    elif name is None:
        if max_workers == 0:
            name = _DEFAULT_SERIAL_ENTRY_POINT_NAME
            entry_point = _SERIAL_ENTRY_POINTS[name]
        else:
            # We accept any non-zero max_workers here, not just positive ones.
            # -1 often means, "pick a good max for me."
            name = _DEFAULT_CONCURRENT_ENTRY_POINT_NAME
            entry_point = _CONCURRENT_ENTRY_POINTS[name]
    else:
        if name in _KNOWN_BUT_UNAVAILABLE_ENTRY_POINTS:
            raise ValueError(
                f"There is a {name!r} ExecutorCtx plugin, but the plugin "
                f"has marked itself as unavailable in this process. Has its "
                f"dependencies been installed?"
            )
        elif max_workers == 0:
            # Be nice and auto-switch to a concurrent one if needed.
            if name in _SERIAL_ENTRY_POINTS:
                entry_point = _SERIAL_ENTRY_POINTS[name]
            elif name in _CONCURRENT_ENTRY_POINTS:
                entry_point = _CONCURRENT_ENTRY_POINTS[name]
                # We only get here for concurrent-only entry points, and those
                # typically raise exceptions if max_workers==0. So silently
                # upgrade to max_workers=1.
                max_workers = 1
            else:
                raise ValueError(
                    f"Unknown serial ExecutorCtx plugin name: {name!r}\n"
                    f"Known names: {sorted(_SERIAL_ENTRY_POINTS)}"
                )
        else:
            # Be nice an auto-switch to a serial one if needed.
            if name in _CONCURRENT_ENTRY_POINTS:
                entry_point = _CONCURRENT_ENTRY_POINTS[name]
            elif name in _SERIAL_ENTRY_POINTS:
                entry_point = _SERIAL_ENTRY_POINTS[name]
                # We only get here for serial-only entry points. TO be
                # consistent with the behavior when auto-switching to
                # concurrent-only entry points, we'll override max_workers.
                max_workers = 0
            else:
                raise ValueError(
                    f"Unknown concurrent ExecutorCtx plugin name: {name!r}.\n"
                    f"Known names: {sorted(_CONCURRENT_ENTRY_POINTS)}"
                )

    return name, max_workers, entry_point


def is_plugin_available(name: str) -> bool:
    """
    Returns whether the given plugin is known and is available.

    >>> is_plugin_available("inline")
    True
    >>> is_plugin_available("not-a-plugin")
    False
    """
    return (name not in _KNOWN_BUT_UNAVAILABLE_ENTRY_POINTS) and (
        (name in _SERIAL_ENTRY_POINTS) or (name in _CONCURRENT_ENTRY_POINTS)
    )


def skip_if_unavailable(name: str):
    """
    Unit test helper that skips the current doctest or formal unit test if the
    given plugin is not available in the current Python installation.
    """
    if not is_plugin_available(name):
        import pytest

        all_plugin_names = sorted(
            set(_CONCURRENT_ENTRY_POINTS) | set(_SERIAL_ENTRY_POINTS)
        )
        known_plugin_names = sorted(_KNOWN_BUT_UNAVAILABLE_ENTRY_POINTS)
        # We need to use pytest.skip and not unittest.SkipTest: the latter
        # breaks doctests, at least for pytest.
        pytest.skip(
            f"The {name!r} plugin is not available.\n"
            f"    available: {all_plugin_names}\n"
            f"    known but unavailable: {known_plugin_names}"
        )


def _interpret_plugin_and_max_workers(
    plugin_or_max_workers: Optional[Union[str, int]] = None,
    /,
    max_workers: Optional[int] = None,
) -> Tuple[str, Optional[int]]:
    """
    Helper for `.get_entry_point` that decodes the first 0-2 positional
    arguments expected by `~pyrseus.ctx.mgr.ExecutorCtx`'s constructor, but
    without inspecting the plugin registry. See the constructor's documentation
    for an in-depth discussion.

    >>> _interpret_plugin_and_max_workers()
    (None, None)
    >>> _interpret_plugin_and_max_workers(0)
    (None, 0)
    >>> _interpret_plugin_and_max_workers(4)
    (None, 4)
    >>> _interpret_plugin_and_max_workers("process")
    ('process', None)
    >>> _interpret_plugin_and_max_workers("process", 4)
    ('process', 4)

    """
    if plugin_or_max_workers is None:
        if max_workers is not None:
            raise ValueError(
                f"{max_workers=} should only be supplied when the "
                f"first argument is a string."
            )
        plugin_name = None
    elif max_workers is None:
        if isinstance(plugin_or_max_workers, str):
            plugin_name = plugin_or_max_workers
        else:
            plugin_name = None
            max_workers = int(plugin_or_max_workers)
    else:
        plugin_name = plugin_or_max_workers

    return plugin_name, max_workers


def filter_kwargs(
    entry_point: ExecutorPluginEntryPoint, kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    """
    This function performs the keyword validation and filtering for
    `~pyrseus.ctx.mgr.ExecutorCtx`.

    It removes keyword arguments from ``kwargs`` that are not accepted by
    ``entry_point``'s `create <.ExecutorPluginEntryPoint.create>` method. To
    help avoid hiding too many errors, this function also raises an exception if
    ``kwargs`` has any keywords that are now allowed by any known entry points.

    Keywords that are supported by the given entry point are preserved.

        >>> _, _, process_ep = get_entry_point("process", 1)
        >>> filter_kwargs(process_ep, {"initializer": None})
        {'initializer': None}

    Keywords that are not supported by any registered entry point result in
    exceptions.

        >>> filter_kwargs(process_ep, {"not-a-keyword-for-any-plugin": 42})
        Traceback (most recent call last):
        ...
        TypeError: Keyword "not-a-keyword-for-any-plugin" is not accepted ...

    Keywords that are not supported by the given entry point but are supported
    by another are silently dropped.

        >>> skip_if_unavailable("loky")
        >>> _, _, loky_ep = get_entry_point("loky", 1)
        >>> filter_kwargs(process_ep, {"reuse": True, "initializer": None})
        {'initializer': None}
        >>> filter_kwargs(loky_ep, {"reuse": True, "initializer": None})
        {'reuse': True, 'initializer': None}
    """
    filtered_kwargs = {}
    for k, v in kwargs.items():
        if k not in _ALL_AVAILABLE_KEYWORDS:
            all_plugin_names = sorted(
                set(_CONCURRENT_ENTRY_POINTS) | set(_SERIAL_ENTRY_POINTS)
            )
            raise TypeError(
                f"Keyword {k!r} is not accepted by any of the registered plugins:\n"
                f"    plugins: {all_plugin_names}\n"
                f"    all allowed keywords: {sorted(_ALL_AVAILABLE_KEYWORDS)}"
            )
        elif k in entry_point.allowed_keywords:
            filtered_kwargs[k] = v
        else:
            # Silently drop those that can be used by other plugins but aren't
            # used by the currently-selected plugin.
            pass

    return filtered_kwargs


def _register_plugins_from_package():
    """
    Helper used at import time to pre-register all plugins found in the
    `pyrseus.ctx.plugins` package.

    Future work: if there's sufficient user demand, add support for either the
    "Using naming convention" or "Using package metadata" approach to
    auto-discovering 3rd party plugins, as described here:
    https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/.
    We previously experimented with using the "Using namespace packages"
    approach, but found it to be too limiting and cumbersome.
    """
    from pyrseus.ctx import plugins

    # This code actually implements the full "Using namespace packages"
    # approach. But to make it work, we'd need to change all of pyrseus,
    # pyrseus.ctx, and pyrseus.ctx.plugins to namespace packages. This would
    # prevent us from providing convenient transplanted imports in the top-level
    # pyrseus module, and prevent us from embedding __version__ there too.
    search_path = plugins.__path__
    search_prefix = plugins.__name__ + "."
    fqns = [fqn for _, fqn, _ in pkgutil.iter_modules(search_path, search_prefix)]
    for fqn in sorted(fqns):  # sort for better repeatability
        mod = importlib.import_module(fqn)
        _, plugin_name = fqn.rsplit(".", 1)
        if not hasattr(mod, "ENTRY_POINT"):
            # For now, we'll be strict and insist that there's no cruft in the
            # pyrseus.ctx.plugins package.
            raise RuntimeError(
                f"The plugin module {fqn} at {mod.__file__} does "
                f"not have an ENTRY_POINT attribute."
            )
        try:
            register_plugin(plugin_name, mod.ENTRY_POINT)
        except Exception as ex:
            raise RuntimeError(
                f"Failed to register the plugin module {fqn} at {mod.__file__}."
            ) from ex


_register_plugins_from_package()

# Right now, we hard-code the initial defaults and the point to ones that ship
# with this package, so they should always be available. We can adjust these
# rules later if there's sufficient user demand.
if _DEFAULT_SERIAL_ENTRY_POINT_NAME not in _SERIAL_ENTRY_POINTS:
    raise ImportError(
        f"The initial default serial plugin, "
        f"{_DEFAULT_SERIAL_ENTRY_POINT_NAME!r} has not been installed. "
        f"All known serial plugins: "
        f"{sorted(_SERIAL_ENTRY_POINTS)}"
    )
if _DEFAULT_CONCURRENT_ENTRY_POINT_NAME not in _CONCURRENT_ENTRY_POINTS:
    raise ImportError(
        f"The initial default concurrent plugin, "
        f"{_DEFAULT_CONCURRENT_ENTRY_POINT_NAME!r} has not been installed. "
        f"All known serial plugins: "
        f"{sorted(_CONCURRENT_ENTRY_POINTS)}"
    )
