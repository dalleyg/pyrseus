
.. _writingplugins:

#######################
Writing Your Own Plugin
#######################

If you'd like to teach `~pyrseus.ctx.mgr.ExecutorCtx` how to use an
`~concurrent.futures.Executor` class, then you'll need to write a new plugin.

Where?
======

The first choice you'll face is where to put your plugin.
We currently supports two options:

- in the Pyrseus repository, in the `pyrseus.ctx.plugins` package

- in your own repository, wherever you want

*In Pyrseus:* Plugins that are added to Pyrseus itself are shipped to all users.
Any user who also installs the optional dependencies required by your plugin's
will automatically be able to use your plugin. E.g. the existing
`~pyrseus.ctx.plugins.mpi4py` plugin is always installed, but it's only
available in a given installation if |mpi4py|_ is also installed.

*Your Repository:* If you use your own repository, users will need to call
`~pyrseus.ctx.registry.register_plugin` to teach `~pyrseus.ctx.mgr.ExecutorCtx`
how to find your plugin. They can do so:

- at the start of each applicable script and/or notebook,

- in an `IPython startup file
  <https://ipython.readthedocs.io/en/stable/interactive/tutorial.html#startup-files>`_,

- in your `usercustomize
  <https://docs.python.org/3/library/site.html#module-usercustomize>`_ module,
  or

- in your `sitecustomize
  <https://docs.python.org/3/library/site.html#module-sitecustomize>`_ module.

Additionally, users can change the default serial and/or concurrent plugin
using `~pyrseus.ctx.registry.SetDefaultExecutorPluginCtx` or
`~pyrseus.ctx.registry.set_default_executor_plugin`.

Checklist
=========

- Decide where you want to put your plugin (see the previous section).

- Decide if you want to use `~pyrseus.ctx.api.ExecutorPluginEntryPoint`
  directly vs. `~pyrseus.ctx.simple.SimpleEntryPoint` as your starting point.
  Pick whichever is easier for your project. See the existing plugins for
  examples of both.

- Decide if you want to support an `~pyrseus.ctx.api.OnError` parameter to your
  entry point's ``create`` method. If so, refer to that enum's documentation.
  Also take a look at the existing plugins to see several different ways of
  implementing the feature.

If Adding to Pyrseus
--------------------

Follow these additional steps if you're contributing the plugin to the Pyrseus
repository itself.

- Create a new file, ``src/pyrseus/plugins/<your-plugin-name>.py``. This file
  must define a global variable called ``ENTRY_POINT`` that implements
  `~pyrseus.ctx.api.ExecutorPluginEntryPoint`'s protocol.

- 3rd party imports *must* be done lazily. Otherwise those 3rd party imports
  suddenly become mandatory imports for Pyrseus. By "lazy", we mean:

  - The plugin's module must not import any 3rd party dependencies at the module
    level.

  - The plugin's ``ENTRY_POINT.is_available`` must not trigger any "3rd party"
    imports. See the existing plugins for how to detect the presence of their
    3rd party dependencies without actually importing any of them.

  - Note: it's fine for ``ENTRY_POINT.allowed_keywords``,
    ``ENTRY_POINT.create``, etc. to trigger the 3rd party imports.

- If any extra 3rd party dependencies are needed, add them to

  - ``optional-dependencies.txt``, if your plugin will work on all platforms,
    or
  - ``optional-non-win32-dependencies.txt``, if your plugin only works on
     Linux (including under WSL) and macOS.

- If any extra 3rd party dependencies are needed, also add them to
  ``docs/requirements.txt`` (always).

- See :doc:`contributing` for additional steps to test your plugin and prepare a
  pull request.

If Adding to a Separate Repository
----------------------------------

If you're creating a plugin in your own codebase, follow these additional
instructions.

- Create a singleton instance object that implements
  `~pyrseus.ctx.api.ExecutorPluginEntryPoint`'s protocol. You can put it
  wherever you want and call it whatever you want.

- To teach Pyrseus about the plugin, each Python process will need to call
  `~pyrseus.ctx.registry.register_plugin` with your entry point instance object.
  See the "Where?" section above for more details.
