Installation
============

.. note::
   This project currently only supports Python 3.7 or 3.8. However, PyPipeline without Ray may work on 3.9 already
   as well.


**Installation from PyPI:**

.. warning::
   TODO: PyPipeline is not yet available on PyPI. See instructions below to install from git.


Install PyPipeline with:

.. code-block:: shell

   $ pip install pypipeline


The dependencies for different options of PyPipeline can be installed separately.
Ex: to install PyPipeline with Ray support + pypipeline_serve dependencies:

.. code-block:: shell

   $ pip install pypipeline[ray, lib_torch]


All possible installation options are:

.. code-block:: text

   ray:         Ray multiprocessing support
   lib_torch:   PyTorch bindings
   serve:       pypipeline_serve for HTTP API generation -> still under development!
   tests:       ray + lib_torch + serve + test requirements
   docs:        to build the docs
   all:         tests + docs


**Installation from github:**

First clone the `PyPipeline repo <https://github.com/JohannesVerherstraeten/pypipeline>`_ from git.

.. code-block:: shell

   git clone git@github.com:JohannesVerherstraeten/pypipeline.git

Then install PyPipeline with all desired options:

.. code-block:: shell

   $ cd pypipeline
   $ pip install -e ".[ray, lib_torch]"
