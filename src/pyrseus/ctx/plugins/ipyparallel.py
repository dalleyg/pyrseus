"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a wrapper around the 3rd
party |ipyparallel|_ executors that (a) provides a more standard executor-style
API, and (b) makes that library safer to use in production library code.

>>> import logging, os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "ipyparallel" can pickle fancy things like lambdas with cloudpickle.
>>> skip_if_unavailable("ipyparallel")
>>> with ExecutorCtx("ipyparallel", 1, log_level=logging.FATAL) as exe:
...     assert exe.__class__.__module__.startswith('ipyparallel')
...     assert exe.submit(lambda: os.getpid()).result() != os.getpid()

For more usage examples, see the `test_ipyparallel_plugin.py
<https://github.com/dalleyg/pyrseus/blob/main/tests/test_ipyparallel_plugin.py>`_
unit tests.

Plugin-specific Notes
---------------------

- *Common Use Cases:* for multi-host workloads, especially for for organizations
  that already have |ipyparallel|_ setup and wish to benefit from
  `~pyrseus.ctx.mgr.ExecutorCtx`'s ability to easily switch to other executors,
  especially the serial ones for light workloads and troubleshooting.

  - If your organization has subclassed or wrapped `.ipyparallel.Cluster` to
    help configure it better, consider deriving your own plugin from this basic
    one. See `.EntryPoint.cluster_module_name` for details.

  - *Non-recommended Use:* for basic single-host workloads, consider using
    `~pyrseus.ctx.plugins.process`, `~pyrseus.ctx.plugins.cpprocess`, or
    `~pyrseus.ctx.plugins.loky` instead of `~pyrseus.ctx.plugins.ipyparallel`.
    They require no setup beyond installation, they have fewer 3rd party package
    dependencies, their startup latency is one to two orders of magnitude lower,
    their workers use the same import environment as the primary process, and
    they have simpler interfaces.

  - *Non-recommended Use:* if you use many of |ipyparallel|_'s advanced features
    (other than configuration overrides), consider continuing to use it directly
    instead of via Pyrseus.

- *Concurrency:* determined by the user's |ipyparallel|_ configuration.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* as of ``ipyparallel`` 7.1.0 on Python 3.10,
  |ipyparallel|_ has 79 total transitive dependencies.

- *Underlying Executors:* miscellaneous wrappers around executors provided by
  |ipyparallel|_.

- *Default max_workers:* determined by the user's |ipyparallel|_ configuration.

- *Pickling:* |ipyparallel|_ is inconsistent about when it enables
  |cloudpickle|_ support. Since it's |ipyparallel|_'s preferred approach and
  since Pyrseus depends on |cloudpickle|_ anyway, this plugin unconditionally
  uses |cloudpickle|_ for pickling.

- *OnError handling:* Only directly supports implicit
  `~pyrseus.ctx.api.OnError.WAIT` semantics.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

import importlib
from contextlib import ExitStack
from functools import cached_property
from typing import Optional

from pyrseus.core.sys import module_exists
from pyrseus.ctx.api import ExecutorPluginEntryPoint, extract_keywords


class _ClusterCtx:
    """
    Automates the creation of a client and its executor from a cluster.
    """

    def __init__(self, cluster, targets):
        # Although we only actually need to keep track of the cluster and the
        # exit stack, we also maintain references to various other objects for
        # troubleshooting.
        self._es = ExitStack()
        self._cluster = cluster
        self._targets = targets
        self._client = None
        self._exe = None

    def __enter__(self):
        self._client = self._es.enter_context(self._cluster)
        self._client[:].use_cloudpickle()
        self._exe = self._client.executor(targets=self._targets)
        entered_exe = self._es.enter_context(self._exe)
        return entered_exe

    def __exit__(self, *exc_info):
        return self._es.__exit__(*exc_info)


class _ClientCtx:
    """
    Automates the creation of a client's executor, given a client.
    """

    def __init__(self, client, targets):
        self._es = ExitStack()
        self._client = client
        self._targets = targets
        self._exe = None

    def __enter__(self):
        self._client[:].use_cloudpickle()
        self._exe = self._client.executor(targets=self._targets)
        entered_exe = self._es.enter_context(self._exe)
        return entered_exe

    def __exit__(self, *exc_info):
        return self._es.__exit__(*exc_info)


class EntryPoint(ExecutorPluginEntryPoint):
    """
    Defines this plugin's entry point so that `~pyrseus.ctx.mgr.ExecutorCtx`
    knows how to use the plugin. This particular plugin is designed with further
    subclassing in mind. See `.cluster_class` and `.allowed_keywords` for
    details.
    """

    supports_serial = False
    supports_concurrent = True

    # NOTE: this cluster_module_name + cluster_class_name design is similar in
    # spirit to the way SimpleEntryPoint works. But we can't use it directly
    # since ipyparallel's interface doesn't directly provide an executor class;
    # users need to use the Cluster and Client classes first, in particular
    # ways, to then obtain an executor.

    cluster_module_name = "ipyparallel"
    """
    Gives the name of the module that contains the `.ipyparallel.Cluster` class
    or its factory that this plugin should use.

    If your organization has its own subclass or factory for clusters, then you
    may wish to create your own plugin that subclasses this one.

    - See :ref:`writingplugins` for general guidance on writing plugins.

    - Override this attribute to refer to your subclass or factory's module
      name.

    - Override the `.cluster_class_name` attribute to refer to your subclass or
      factory's name.

    - If your class uses traits the same way that `.ipyparallel.Cluster` does,
      that's all you should need to do. But if it's a factory function or if's a
      subclass it consumes constructor parameters differently, override
      `.allowed_keywords` as well. See
      `pyrseus.ctx.api.ExecutorPluginEntryPoint.allowed_keywords` for details.
    """

    cluster_class_name = "Cluster"
    """
    Name of the class or factory to import for creating `ipyparallel.Cluster`
    instances. See `.cluster_module_name` for details.
    """

    @cached_property
    def is_available(self) -> bool:
        return module_exists(self.cluster_module_name)

    @cached_property
    def ClusterCls(self) -> type:
        mod = importlib.import_module(self.cluster_module_name)
        return getattr(mod, self.cluster_class_name)

    @cached_property
    def allowed_keywords(self):
        # Merge the extra keywords that our create method consumes with the ones
        # that Cluster accepts. We remove "n" because our framework calls the
        # same thing "max_workers".
        ours = extract_keywords(EntryPoint.create)
        theirs = set(self.ClusterCls._traits)
        renamed = {"n"}
        return (ours | theirs) - renamed

    def create(
        self,
        max_workers: Optional[int] = None,
        *,
        client=None,
        cluster=None,
        targets=None,
        **cluster_kwargs,
    ):
        """
        Creates an `~concurrent.futures.Executor` that uses an |ipyparallel|_
        cluster and client pair as its backend.

        :param max_workers: an optional override to the maximum number of
            workers (engines in |ipyparallel|_ parlance) to use. This argument
            is silently ignored if either ``client`` or ``cluster`` is supplied.

        :param client: an optional `.ipyparallel.Client` object. If supplied,
            this client's default executor will be used by
            `~pyrseus.ctx.mgr.ExecutorCtx`. All other arguments except
            ``targets`` will be silently ignored.

        :param cluster: an optional `.ipyparallel.Cluster` object. Ignored if
            ``client`` is provided. Otherwise, if supplied, a client will be
            created from this cluster, and its default executor will be used by
            `~pyrseus.ctx.mgr.ExecutorCtx`. All other arguments except
            ``targets`` will be silently ignored.

        :param targets: an optional override that selects which |ipyparallel|_
            engines can be used by the executor. See
            `.ipyparallel.Client.executor` for details. If supplied, this
            argument is always honored.

        :param cluster_kwargs: additional keyword arguments passed to
            `.ipyparallel.Cluster`'s constructor if neither ``client`` nor
            ``cluster`` are provided.
        """
        if client is not None:
            return _ClientCtx(client, targets), None
        elif cluster is not None:
            return _ClusterCtx(cluster, targets), None
        else:
            cluster = self.ClusterCls(n=max_workers, **cluster_kwargs)
            return _ClusterCtx(cluster, targets), None


ENTRY_POINT = EntryPoint()
