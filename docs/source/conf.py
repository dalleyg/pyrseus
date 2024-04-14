# Configuration file for the Sphinx documentation builder.

import sys
import time

from jupytext.cli import jupytext
from sphinx.ext import autodoc

# -- Tell sphinx where to find all of our code and tests.

# Insert this clone's copy of Pyrseus first, just in case there is another,
# older version installed.
sys.path[:] = ["../../src", "../../tests/"] + sys.path

# -- Hack to create paired notebook files so that myst_nb can find them.

jupytext(("--update", "--to", "ipynb", "notebooks/*.py"))

# -- Project information

# If this import fails, rerun configure.py at top of the repo.
from pyrseus import __version__ as version_str  # NOQA

project = "pyrseus"
copyright = f"2023-{time.strftime('%Y')}, Gerald Dalley"
author = "Gerald Dalley"

release = ".".join(version_str.split(".")[:2])
version = version_str

# -- General configuration

extensions = [
    "myst_nb",
    "sphinx_design",
    "sphinx_inline_tabs",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "sphinx.ext.duration",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]
autosummary_generate = True

intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]

highlight_language = "python3"

default_role = "autolink"

sphinx_tabs_disable_tab_closing = True
sphinx_tabs_disable_css_loading = True

# Hack from https://stackoverflow.com/a/75041544 to suppress bloated docs about
# things inherited from the base `object` type.


class MockedClassDocumenter(autodoc.ClassDocumenter):
    def add_line(self, line: str, source: str, *lineno: int) -> None:
        if line == "   Bases: :py:class:`object`":
            return
        super().add_line(line, source, *lineno)


autodoc.ClassDocumenter = MockedClassDocumenter

# For cross references to other packages we interface with.
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
    "dill": ("https://dill.readthedocs.io/en/latest", None),
    "ipyparallel": ("https://ipyparallel.readthedocs.io/en/latest", None),
    "loky": ("https://loky.readthedocs.io/en/stable", None),
    "mpi4py": ("https://mpi4py.readthedocs.io/en/stable", None),
}

# -- Options for HTML output

html_theme = "furo"
html_static_path = ["_static"]
html_css_files = [
    "css/custom.css",
]

rst_epilog = """
.. |nbh| unicode:: U+2011
   :trim:
.. |nbsp| unicode:: U+00A0
   :trim:
.. |br| raw:: html

   <br/>
.. |nobr| raw:: html

   <nobr>
.. |nobrc| raw:: html

   </nobr>

.. |cloudpickle| replace:: cloudpickle
.. _cloudpickle: https://github.com/cloudpipe/cloudpickle

.. |dill| replace:: dill
.. _dill: https://dill.readthedocs.io

.. |ipyparallel| replace:: ipyparallel
.. _ipyparallel: https://ipyparallel.readthedocs.io

.. |loky| replace:: loky
.. _loky: https://loky.readthedocs.io

.. |mpi4py| replace:: mpi4py
.. _mpi4py: https://mpi4py.readthedocs.io

.. |psutil| replace:: psutil
.. _psutil: https://psutil.readthedocs.io

.. |nb_get_executor| replace:: ExecutorCtx vs. get_executor
.. _nb_get_executor: ../notebooks/get_executor.html

"""

# -- Options for EPUB output
epub_show_urls = "footnote"
