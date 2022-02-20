Scale-up basics
===============

Intro
-----

Imagine that the cells in our pipeline example would perform some heavy, time-consuming operations, instead of just
simple integer arithmetic. The source cell would need 1 second to execute, the multiplier 4 and the printer 2 seconds.
Because of their combined execution times, the throughput of the whole pipeline would be limited to
**1/7 iterations/s**.

.. figure:: /data/scaleup-threading-0-3.png

   A simple pipeline that produces two integers, multiplies them, and prints the result to screen. The (imaginary)
   execution time of each cell is given below the name. On the right, the concurrency diagram is visualized.
   Without a scalable cell, none of the cells run concurrently.


One of the core features of PyPipeline is the ability to scale-up bottleneck cells, allowing to boost the
throughput of your pipelines.


.. figure:: /data/pypipeline-docs-getting-started-pipeline_new_scaleup.png

   A scalable cell introduces pipeline parallelism and allows to duplicate bottleneck cells.


Scalable cells
--------------

A scalable cell is very similar to a pipeline: it is also a composite cell. But unlike a pipeline, a scalable
cell can only have 1 internal cell. This cell can be scaled up from 0 to infinity, as long as you have enough
resources to run it.

The internal cell doesn't do any operation on the data itself. Instead, its work is divided over all of its clones.
Each clone runs in a different thread, parallel to the other clones. The clones can only one at a time
pull the inputs of the scalable cell. This ensures that the prerequisite cells will never
be pulled concurrently.

A scalable cell can be defined as follows:

.. code-block:: python
   :emphasize-lines: 19

    from pypipeline.cell import ScalableCell

    class ScalableMultiplierCell(ScalableCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(ScalableMultiplierCell, self).__init__(parent_cell, name=name)

            self.multiplier = MultiplierCell(self, "multiplier")

            self.input_port_a: InputPort[int] = InputPort(self, "input_a")
            self.input_port_b: InputPort[int] = InputPort(self, "input_b")
            self.output_port: OutputPort[int] = OutputPort(self, "output")

            Connection(self.input_port_a, self.multiplier.input_a)
            Connection(self.input_port_b, self.multiplier.input_b)
            Connection(self.multiplier.output, self.output_port)

    scalable_multiplier = ScalableMultiplierCell(None, "test_scalable_multiplier")
    scalable_multiplier.scale_up(times=2)
    scalable_multiplier.deploy()

In the example at the bottom of this page, the full pipeline with this scalable cell is implemented.

a) Not scaled-up
................

When a scalable cell is not scaled up, it has no clones that can do the internal cell's operation. Therefore,
no data will be able to flow through the pipeline: the pipeline will have no throughput at all.

.. figure:: /data/scaleup-threading-0-2.png

   A scalable cell that is not scaled up, fully blocks the pipeline.


Execution results:

.. code-block:: text

   pypipeline.exceptions.InactiveException: ScalableMultiplierCell("test_scalable_multiplier").ScalableCellDeployment() is being pulled, but got inactive.
   Inactivity is caused by having no (active) scale-up worker threads. Did you scale up this cell at least once?


b) Scaled-up once
.................

Scaling up the cell once, will introduce a separate thread in which the clone cell will run. The thread
boundary is drawn with a vertical dashed line at the output side of the scalable cell. Everything before
the line is executed in the separate clone thread. Everything after the line, in this case the printer cell,
is executed in the main thread.

The boundary line of the scalable cell contains a queue. The clone thread will put its result in the queue,
and signal the next cells that a new output is available. While the next cells start using this output, the
clone has already begun its next cycle.

Therefore, even though the scalable cell has only one active worker, we already see some degree of concurrency
in the concurrency diagram. This type of concurrency is called "pipeline parallelism". It increases the
throughput of our pipeline to **1/5 iterations/s**.

.. figure:: /data/scaleup-threading-1-3.png

   A scalable cell that is scaled up once, introduces pipeline parallelism. In the concurrency diagram on the
   right, we see how the clone thread (yellow) interacts with the main thread (blue) when the pipeline is
   being pulled.

Execution results:

.. code-block:: text

   0     (7s)
   -1    (5s)
   2     (5s)
   -3    (5s)
   4     (5s)


c) Scaled-up twice or more
..........................

Cells that do not modify their internal state based on the data coming in, can be scaled up even more.
In that case, incoming data will be divided over multiple clones of the internal cell, each running in parallel.

.. note::
   Cells that do not modify their internal state, are also called "pure functions" or cells without side
   effects. Only those cells should be scaled up twice or more.

Even though multiple clones of the internal cell run in parallel, the inputs of the scalable cell will
only be pulled by one thread at a time, to make sure the source cells aren't pulled concurrently.

PyPipeline will always make sure that the data comes out of the scalable cell, in the same ordering as the input data,
even when some clones work faster/slower than the others. This is an important feature, when the scalable
cell is followed by a stateful cell that needs the input in correct order.

In our example, duplicating the multiplier cell twice, increases the throughput of our pipeline to
**2/5 iterations/s**.

.. figure:: /data/scaleup-threading-2-3.png

   A scalable cell introduces pipeline parallelism and allows to duplicate bottleneck cells. In the concurrency
   diagram on the right, we see the two clone threads pulling the source one after another. The execution of the
   multiplier cell happens concurrently.


Execution results:

.. code-block:: text

   0     (7s)
   -1    (2s)
   2     (3s)
   -3    (2s)
   4     (3s)


Example
-------

.. code-block:: python
   :emphasize-lines: 3,19,38,42,66,86

    from typing import Optional
    import time
    from pypipeline.cell import ASingleCell, ACompositeCell, Pipeline, ScalableCell
    from pypipeline.cellio import Output, Input, InputPort, OutputPort
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
            time.sleep(1)
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
            time.sleep(4)
            self.output.set_value(result)


    class ScalableMultiplierCell(ScalableCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(ScalableMultiplierCell, self).__init__(parent_cell, name=name)

            self.multiplier = MultiplierCell(self, "multiplier")

            self.input_port_a: InputPort[int] = InputPort(self, "input_a")
            self.input_port_b: InputPort[int] = InputPort(self, "input_b")
            self.output_port: OutputPort[int] = OutputPort(self, "output")

            Connection(self.input_port_a, self.multiplier.input_a)
            Connection(self.input_port_b, self.multiplier.input_b)
            Connection(self.multiplier.output, self.output_port)


    class PrinterCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(PrinterCell, self).__init__(parent_cell, name=name)
            self.input: Input[int] = Input(self, "printer_input")

        def _on_pull(self) -> None:
            value = self.input.pull()
            time.sleep(2)
            print(value)


    class MyPipeline(Pipeline):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MyPipeline, self).__init__(parent_cell, name=name)

            self.source = SourceCell(parent_cell=self, name="source")
            # Note that we leave out the redundant `parent_cell=` and `name=` keywords from here on...
            self.scalable_multiplier = ScalableMultiplierCell(self, "scalable_multiplier")
            self.printer = PrinterCell(self, "printer")

            Connection(self.source.output_1, self.scalable_multiplier.input_port_a)
            Connection(self.source.output_2, self.scalable_multiplier.input_port_b)
            Connection(self.scalable_multiplier.output_port, self.printer.input)


    pipeline = MyPipeline(parent_cell=None, name="my_pipeline")
    pipeline.scalable_multiplier.scale_up(times=2)
    pipeline.deploy()

    for i in range(5):
        t0 = time.time()
        pipeline.pull()
        t1 = time.time()
        print(t1 - t0)

    pipeline.undeploy()     # to shutdown threads


Scaling up with threading / Ray
-------------------------------

PyPipeline currently supports two types of parallelism:
 * Multithreading (default)
 * Multiprocessing with `Ray actors <https://docs.ray.io/en/master/index.html>`_

Switching between the two types of parallelism is as easy as changing the ``cell.scale_up(method=...)`` parameter,
see example below.

The Ray approach will run your cell in a Ray actor, which runs in a different process. Ray provides efficient
inter-process communication, certainly when using bigger data (like images, big numpy arrays, ...).

The advantage of using Ray, is that the concurrency of your pipelines isn't locked anymore by the python global
interpreter lock (GIL). (TODO more detailed explanation)

The disadvantage of any multi-processing approach, is that the inter-process communication isn't as efficient as
inter-thread communication. This is because threads share the same memory space, allowing them to pass references
to the objects in memory directly. Processes have their own separate memory space, and have to package and transfer
the data to each other's.

The general rule of thumb is: use multi-processing (Ray) for CPU intensive tasks, and use multi-threading for tasks
that perform a lot of I/O (ex: disk read/writes, HTTP calls, GPU operations?). But don't hesitate to try which one
delivers the best throughput for your application!

A performance comparison on some real-life usecases can be found under Advanced Topics > Performance tests.

.. code-block:: python

    from pypipeline.cell import ThreadCloneCell, RayCloneCell

    # Note: the `method` parameter definition is still likely to change in the coming months.
    # Note: you can scale up a scalable cell with both methods at the same time:
    pipeline.scalable_multiplier.scale_up(times=2, method=ThreadCloneCell)
    pipeline.scalable_multiplier.scale_up(times=2, method=RayCloneCell)


Scaling up a pipeline
---------------------

Scalable cells are a type of composite cell that are only allowed to have 1 internal cell. This internal
cell is, like all composite cells, of type ``ACell``.

.. figure:: /data/pypipeline-docs-getting-started-nesting-uml-scalable.png

   A (simplified) UML diagram of the cell class hierarchy.


As a consequence, every type of cell can be scaled up
with a scalable cell, even composite cells. You can scale up a full pipeline consisting of multiple cells (the
full pipeline will run in a different thread). And even more, you can scale up other scalable cells, or full pipelines
with other scalable cells inside (still experimental).

Note that the same rule still applies: cells (or pipelines) that get scaled up more than once, should not modify
any internal state.

.. figure:: /data/pypipeline-docs-getting-started-scaling-nested.png

   An example of a pipeline nested inside a scalable cell. Try to build it yourself!


Multiple scalable cells
-----------------------

You can connect multiple scalable cells inside a pipeline. You could wonder how all their clones would interact
with each other. For example, would the source cell in the following figure not be pulled concurrently, as it
has two scalable cells pulling in separate threads? Would this not introduce race conditions?

.. figure:: /data/pypipeline-docs-getting-started-scaling-multiple.png

   Multiple scalable cells connected inside one pipeline.

Don't worry, PyPipeline handles all threadsafety issues for you. In this case, it will make sure that
the source cell is only executed by one thread at a time, and that all cells see the source data exactly
once, even when their respective throughputs differ a lot. This is achieved by the use of an extensive locking
system in both the cells themselves and their outputs.


Skip connections
----------------

An interesting case is the following. In your final sink cell, you need some data that is provided by the
source cell, but isn't needed in the scalable operation cell. You can define your pipeline as follows.

.. figure:: /data/pypipeline-docs-getting-started-scaling-skipconnection-bad.png

   A pipeline with a connection that passes a scalable cell. The connection circumvents the thread boundary,
   effectively "coupling" the execution speed of the clone threads and the main thread, disabling any concurrency.

The disadvantage of this pipeline definition, is that scaling up the operation cell won't have any impact on the
throughput of the whole pipeline. Imagine the clones of the scalable operation trying to pull the source
cell. The source can only give a new value, when the sink has seen the value from the direct connection too. The
direct connection acts as a kind of synchronizer: the scalable cell must run with the same speed as the source cell,
and they both get pulled with the speed of the sink cell. No concurrent execution will take place.

You can solve this by adding a skip connection inside the scalable cell. The skip connection does pass through the
thread boundary (with its queuing system), making sure that the source and the sink are fully decoupled thread-wise.

.. figure:: /data/pypipeline-docs-getting-started-scaling-skipconnection-good.png

   A pipeline with a skip connection inside the scalable cell. The connection now correctly passes through the
   thread boundary, allowing concurrency in the pipeline.