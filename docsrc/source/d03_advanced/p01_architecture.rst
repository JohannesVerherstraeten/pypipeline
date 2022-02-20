Architecture
============

TODO add design decisions document (see docs/source/design_decisions.md).

High level overview
-------------------

PyPipeline consists of three main concepts:
 - Cells, inheriting the ``ICell`` interface,
 - Cell IO, inheriting the ``IO`` interface,
 - Connections, inheriting the ``IConnection`` interface.

This is visible in the UML diagram of the toplevel interfaces of PyPipeline:


.. figure:: /data/pypipeline-docs-advanced-architecture-global-uml.png

   UML class diagram of the toplevel interfaces in PyPipeline.


Cells have inputs ``IInput`` in which data comes in, and outputs ``IOutput`` in which data leaves the cell.

Connections start at ``IConnectionExitPointIO`` and arrive at ``IConnectionEntryPointIO``. We differentiate between
inputs and connection entrypoints on one hand, and outputs and connection exitpoints on the other hand, since some
inputs and outputs both allow incoming/outgoing connections at the same time (ex: ``InputPort``, ``OutputPort``).

The following sections each elaborate on the design of one of the three main concepts.

Cell class hierarchy
--------------------

TODO update: add ``ICompositeCell`` interface.

All cells in PyPipeline inherit the ``ICell`` interface as follows:

.. figure:: /data/pypipeline-docs-advanced-architecture-cell-uml.png

   UML class diagram of the cell hierarchy.


The ``ACell`` abstract class gathers all functionality that is common to all cells (ex: cell name, parent cell,
inputs/outputs).

The ``ACompositeCell`` and ``ASingleCell`` are the two main types of cells. They form the
`composite design pattern <https://en.wikipedia.org/wiki/Composite_pattern>`_, allowing to nest cells into each other.


There are three types of composite cells:
 - a ``Pipeline``, allowing to connect the internal cells to each other,
 - a ``ScalableCell``, allowing to scale up 1 internal cell by creating multiple clones of it,
 - a ``ACloneCell``, which manages 1 clone of a scalable cell.

A ``ScalableCell`` has both an internal cell, which does not perform any operation on the incoming data, and zero or more
clone cells that all contain a clone of the internal cell. It's the clones that perform the operations on the data.

PyPipeline at the moment supports two different kinds of scale-up clones: ``RayCloneCell`` running the internal cell in
a Ray actor (=a separate process), and ``ThreadCloneCell`` running the internal cell in a separate thread.

.. figure:: /data/pypipeline-docs-advanced-architecture-scalable-component.png

   Diagram showing the relation between a scalable cell, its internal cell and its clones.


Cell IO class hierarchy
-----------------------

The cell IO class hierarchy is a bit more complex. The main idea is that you have inputs (inheriting ``AInput``
and therefore ``IInput`` too), and outputs (inheriting ``AOutput`` and therefore ``IOutput`` too).
Then every type of input and output may additionally implement the ``IConnectionEntryPoint`` or ``IConnectionExitPoint``
interface as well, depending on whether it can accept incoming or outgoing connections.


To implement the ``IConnectionEntryPoint`` or ``IConnectionExitPoint`` interfaces, classes make use of the
`composition over inheritance pattern <https://en.wikipedia.org/wiki/Composition_over_inheritance>`_. They can make use
of the ``ConnectionEntryPoint``, ``ConnectionExitPoint`` or ``RecurrentConnectionExitPoint`` class.

.. figure:: /data/pypipeline-docs-advanced-architecture-io-uml.png

   UML class diagram of cell IO hierarchy.


Connection hierarchy
--------------------

The connection class hierarchy is the easiest one: for now, there only exists one concrete connection type.

.. figure:: /data/pypipeline-docs-advanced-architecture-connection-uml.png

   UML class diagram of cell connection hierarchy.
