Cell nesting
============

Intro
-----

The basic pipeline example shows the first usecase of nesting cells inside a pipeline.

Visually, we represent cell nesting by drawing them inside the parent object (the pipeline).

.. figure:: /data/pypipeline-docs-getting-started-pipeline_new.png

   The basic pipeline example, built in previous section. Three cells are nested inside one pipeline.

In code, cells are nested when they get a ``parent_cell`` argument during initialization:

.. code-block:: python

    class MyPipeline(Pipeline):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            ...
            # Every cell is created with a parent_cell (self in this case) and a name
            # Note that we leave out the argument names after the first cell.
            self.source = SourceCell(parent_cell=self, name="source")
            self.multiplier = MultiplierCell(self, "multiplier")
            self.printer = PrinterCell(self, "printer")
            ...


Based on the typing info, you might have come to the conclusion that a ``Pipeline`` object must be an instance
of an ``ACompositeCell``. This is indeed the case! Let's formalize this.


Cell class hierarchy
--------------------

PyPipeline has three types of main building blocks to assemble complex applications: cells, cell IO
(inputs and outputs), and connections. We've already seen three types of cells as well: ``ASingleCell``,
``ACompositeCell`` and a ``Pipeline``. The following UML class diagram explains the relations between these
different types of cells.

.. figure:: /data/pypipeline-docs-getting-started-nesting-uml-pipeline.png

   A (simplified) UML class diagram of the relation between cells and pipelines.

All cells inherit from the ``ACell`` class. It's leading ``A`` indicates it is an *abstract* class, meaning it
should only be inherited from, never instantiated directly. This class gathers the functionality that is common
to all cells in PyPipeline. For example, it keeps track of the ``parent_cell`` property of your cells, as well
as the cell's inputs and outputs, and much more.

There are two subtypes of ``ACell``: ``ASingleCell`` and ``ACompositeCell``. The former one is the easiest,
and represents all cells that do some single operation. The latter one is a type of cell that can aggregate other cells.
These other cells are said to be "nested" inside the composite cell. The composite cell composes the operations
of all its internal cells into one large operation. This design pattern is called the
`Composite Pattern <https://en.wikipedia.org/wiki/Composite_pattern>`_.

A ``Pipeline`` is the first type of composite cell that we've seen so far. We'll come in contact with more
composite cell types when we'll start scaling up parts of our pipeline.


Example
-------

A cool aspect of the composite pattern, is that you can nest cells endlessly deep! Indeed, the internal cells
of a composite cell don't necessarily have to be single cells. They can be composite cells as well.

Also, composite cells (and therefore pipelines as well) inherit from the ``ACell`` class, so they inherit the
functionality to have inputs and outputs too. Composite cells allow a new type of inputs and outputs: the
``InputPort`` and ``OutputPort``. They both allow incoming and outgoing connections, functioning as
data ports to the internal cells of the composite cell.

This allows us to build pipelines like this:

.. figure:: /data/pypipeline-docs-getting-started-pipeline-nesting.png

   A nested pipeline, with inputs and outputs. This can be useful when you want to reuse your nested pipeline
   in other pipelines with different sources and sinks.


The code for this example is given here. The ``SourceCell``, ``MultiplierCell`` and ``PrinterCell`` are the same
as in the basic pipeline example.

.. code-block:: python

    from typing import Optional
    from pypipeline.cell import ASingleCell, ACompositeCell, Pipeline
    from pypipeline.cellio import Output, OutputPort, Input, InputPort
    from pypipeline.connection import Connection


    class SourceCell(ASingleCell):
        ...

    class MultiplierCell(ASingleCell):
        ...

    class PrinterCell(ASingleCell):
        ...


    class MyNestedPipeline(Pipeline):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MyNestedPipeline, self).__init__(parent_cell, name=name)

            self.input_a: InputPort[int] = InputPort(self, "input_a")
            self.input_b: InputPort[int] = InputPort(self, "input_b")
            self.output: OutputPort[int] = OutputPort(self, "nested_pipeline_output")

            self.multiplier_1 = MultiplierCell(self, "multiplier_1")
            self.multiplier_2 = MultiplierCell(self, "multiplier_2")

            Connection(self.input_a, self.multiplier_1.input_a)
            Connection(self.input_b, self.multiplier_1.input_b)
            Connection(self.input_b, self.multiplier_2.input_b)
            Connection(self.multiplier_1.output, self.multiplier_2.input_a)
            Connection(self.multiplier_2.output, self.output)


    class MyPipeline(Pipeline):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MyPipeline, self).__init__(parent_cell, name=name)

            self.source = SourceCell(self, "source")
            self.nested_pipeline = MyNestedPipeline(self, "my_nested_pipeline")
            self.printer = PrinterCell(self, "printer")

            Connection(self.source.output_1, self.nested_pipeline.input_a)
            Connection(self.source.output_2, self.nested_pipeline.input_b)
            Connection(self.nested_pipeline.output, self.printer.input)


    pipeline = MyPipeline(parent_cell=None, name="my_pipeline")
    pipeline.deploy()

    for i in range(5):
        pipeline.pull()


Output:

.. code-block:: text

    0
    1
    2
    3
    4
