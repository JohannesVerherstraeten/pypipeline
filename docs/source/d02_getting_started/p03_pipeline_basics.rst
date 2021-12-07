Pipeline basics
===============

Pipeline example
----------------

Let's now build the following pipeline:

.. figure:: /data/pypipeline-docs-getting-started-pipeline_new.png

   A simple pipeline that produces two integers, multiplies them, and prints the result to screen.


You can find the full implementation of this pipeline here, a step-by-step explanation is given afterwards.


.. code-block:: python

    from typing import Optional
    from pypipeline.cell import ASingleCell, ACompositeCell, Pipeline
    from pypipeline.cellio import Output, Input
    from pypipeline.connection import Connection


    class SourceCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(SourceCell, self).__init__(parent_cell, name=name)
            self.output_1: Output[int] = Output(cell=self, name="first_output")
            self.output_2: Output[int] = Output(cell=self, name="second_output")
            self._counter: int = 0

        def _on_pull(self) -> None:
            value_1 = self._counter
            value_2 = 1 if (self._counter % 2 == 0) else -1
            self.output_1.set_value(value_1)
            self.output_2.set_value(value_2)
            self._counter += 1


    class MultiplierCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MultiplierCell, self).__init__(parent_cell, name=name)
            # Note that we leave out the redundant `cell=` and `name=` keywords from here on...
            self.input_a: Input[int] = Input(self, "input_a")
            self.input_b: Input[int] = Input(self, "input_b")
            self.output: Output[int] = Output(self, "output")

        def _on_pull(self) -> None:
            value_a = self.input_a.pull()
            value_b = self.input_b.pull()
            result = value_a * value_b
            self.output.set_value(result)


    class PrinterCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(PrinterCell, self).__init__(parent_cell, name=name)
            self.input: Input[int] = Input(self, "printer_input")

        def _on_pull(self) -> None:
            value = self.input.pull()
            print(value)


    class MyPipeline(Pipeline):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MyPipeline, self).__init__(parent_cell, name=name)

            self.source = SourceCell(parent_cell=self, name="source")
            # Note that we leave out the redundant `parent_cell=` and `name=` keywords from here on...
            self.multiplier = MultiplierCell(self, "multiplier")
            self.printer = PrinterCell(self, "printer")

            Connection(self.source.output_1, self.multiplier.input_a)
            Connection(self.source.output_2, self.multiplier.input_b)
            Connection(self.multiplier.output, self.printer.input)


    pipeline = MyPipeline(parent_cell=None, name="my_pipeline")
    pipeline.deploy()

    for i in range(5):
        pipeline.pull()



Step-by-step
------------

Let's break down the implementation line by line.


.. code-block:: python

    from typing import Optional
    from pypipeline.cell import ASingleCell, ACompositeCell, Pipeline
    from pypipeline.cellio import Output, Input
    from pypipeline.connection import Connection

You'll notice that PyPipeline heavily uses the python type hinting functionality
(`what is type hinting? <https://stackoverflow.com/questions/32557920/what-are-type-hints-in-python-3-5>`_).
This is never an obligation, but it helps a lot to find bugs sooner. In this case we
use the ``Optional[Type]`` type hint, to denote that a variable may be of type ``Type`` or None.
The other imports show the 3 main building blocks of PyPipeline: cells, cell io (inputs and outputs) and connections.


SourceCell
...........

.. code-block:: python

    class SourceCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(SourceCell, self).__init__(parent_cell, name=name)
            self.output_1: Output[int] = Output(cell=self, name="first_output")
            self.output_2: Output[int] = Output(cell=self, name="second_output")
            self._counter: int = 0

        def _on_pull(self) -> None:
            value_1 = self._counter
            value_2 = 1 if (self._counter % 2 == 0) else -1
            self.output_1.set_value(value_1)
            self.output_2.set_value(value_2)
            self._counter += 1

The source cell overrides the ``__init__`` to add some extra properties. More specifically, it adds two output
objects and a counter value. The first output is stored at ``self.output_1``. You can give it any name you want,
this attribute name has no specific meaning to PyPipeline. You only need it to be able to make connections from
it later on.  Our output objects have type ``Output[int]`` meaning that they will output integer values. They're
created with a reference to the cell itself (=``self``) and a name: ``Output(self, "some_name")``. Again, the name
is internally used as unique identifier, therefore a cell cannot have multiple inputs or outputs with the same name.

During its pull, the cell generates two values, provides these values to the outputs, and increments the counter by
one. All connections that will be made to an output, will be able to forward its value to the next cells.

Do you find the ``__init__`` rather verbose because of the type hinting? Note that, although it is encouraged as good
practice, you are completely free in using it. PyPipeline will work just as well without it:

.. code-block:: python

    class SourceCell(ASingleCell):

        def __init__(self, parent_cell, name):
            super(SourceCell, self).__init__(parent_cell, name)
            # You can also leave out the redundant `cell=` and `name=` keywords:
            self.output_1 = Output(self, "first_output")
            self.output_2 = Output(self, "second_output")
            self._counter = 0


MultiplierCell
..............

.. code-block:: python

    class MultiplierCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MultiplierCell, self).__init__(parent_cell, name=name)
            # Note that we leave out the redundant `cell=` and `name=` keywords from here on...
            self.input_a: Input[int] = Input(self, "input_a")
            self.input_b: Input[int] = Input(self, "input_b")
            self.output: Output[int] = Output(self, "output")

        def _on_pull(self) -> None:
            value_a = self.input_a.pull()
            value_b = self.input_b.pull()
            result = value_a * value_b
            self.output.set_value(result)

Our multiplier has both an input and output. During a pull, the multiplier will pull its input to request the next
input value, it will multiply the value by 2, and set the output value to the result.

.. note::
   During a pull, a cell must pull its inputs, perform its functionality, and set its outputs.


This gives away the execution strategy that PyPipeline uses to execute multi-cell pipelines: pull-based execution.

1. A cell pulls its inputs one by one,
2. When an input is pulled, it will pull its incoming connection,
3. When a connection is pulled, it pulls its exit point (usually the output of the previous cell),
4. When an output is pulled, it will check whether its value is already set, or will pull the cell to which it belongs.

This way, when a pull is performed on the last cell in your pipeline, it propagates back up to the first cells, just
like you would pull water through a pipeline using negative pressure. More about this topic will follow later.


PrinterCell
...........

.. code-block:: python

    class PrinterCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(PrinterCell, self).__init__(parent_cell, name=name)
            self.input: Input[int] = Input(self, "printer_input")

        def _on_pull(self) -> None:
            value = self.input.pull()
            print(value)

This cell is nothing special anymore. Let's put all cells together into a pipeline!


Putting it all together
.......................

.. code-block:: python

    class MyPipeline(Pipeline):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MyPipeline, self).__init__(parent_cell, name=name)

            self.source = SourceCell(parent_cell=self, name="source")
            # Note that we leave out the redundant `parent_cell=` and `name=` keywords from here on...
            self.multiplier = MultiplierCell(self, "multiplier")
            self.printer = PrinterCell(self, "printer")

            Connection(self.source.output_1, self.multiplier.input_a)
            Connection(self.source.output_2, self.multiplier.input_b)
            Connection(self.multiplier.output, self.printer.input)

As you can see, a pipeline doesn't inherit from ``ASingleCell`` but from the ``Pipeline`` class. Our pipeline
creates an instance of each of our three cells. Note that this time, the ``parent_cell`` of these three is the pipeline
itself instead of None. The cells are "nested" inside the pipeline, also called "the internal cells" of ``MyPipeline``.

The pipeline also defines the connections between the inputs and outputs of its internal cells. These connections are
unidirectional: they'll channel data from the first argument to the second one.

Ok we've everything we need, let's instantiate our pipeline and start executing it!

.. code-block:: python

    pipeline = MyPipeline(parent_cell=None, name="my_pipeline")
    pipeline.deploy()

    for i in range(5):
        pipeline.pull()


Note that we didn't define the ``_on_pull()`` method for our pipeline. This is already defined in the ``Pipeline`` class
itself. It will make sure that all cells are pulled in the right order.

Output:

.. code-block:: text

    0
    -1
    2
    -3
    4
