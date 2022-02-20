Pull-based execution
====================

Concept
-------

Pipelines are executed in a pull-based way. This is similar to a water pipeline system: you open the water tap,
and because of the negative pressure outside of the pipeline (wrt the inside), water gets pulled out. This
creates a negative pressure inside the water tap itself, causing water to be pulled from the whole pipeline system
to the tap.

PyPipeline processes data streams in a similar way. Pulling always starts at the sink cells, the last cells in your
pipeline. A cell that is being pulled, pulls its inputs as a way to request a new input value. Some inputs (ex:
``RuntimeParameter``) can provide a value immediately. Other inputs (ex: ``Input``) will pull their incoming connection
to request their new value. The connections pull the outputs from where they leave, requesting the cell before it to
pull as well. This pulling propagates back up to the source cells that effectively provide the data.



.. figure:: /data/pypipeline-docs-pulling-a.gif

   This gif shows the pull propagation in a pipeline. Red: busy with pulling. Yellow: ready pulling.
   Note how output objects remember which connection already have pulled and which haven't.


General rules
-------------

The following rules make sure that, when cells get parallelized and scaled up over multiple threads and processes, no
cell misses parts of the input data or sees the same input data twice.

**When a cell is pulled, it should pull its inputs, execute its operation, and set its outputs.**


**Pulling an output object as a connection, is threadsafe.**

It is safe for multiple connections to pull the same output object from different threads. The pulls will be
handled one after another, while next ones will be blocked until it's their turn.


**Output objects keep track of which connections already have pulled and which haven't.**

If an output object is being pulled by a connection, the pull will be blocked if any other connection still has to
pull the previous value. In other words: every connection from an output object can successfully pull exactly once,
before any of them can pull again. Every connection pulling before its turn, gets blocked until all others have pulled.
While a pulling connection is blocked, other connections may pull from different threads.


**Pulling a cell as an output object, is threadsafe.**

It is safe for multiple output objects of a cell, to pull to pull that cell from different threads. The pulls will be
handled one after another, while the next ones will be blocked until it's their turn.


Advanced control flow
---------------------

In some cases, you might want to deviate from the standard "pull inputs, perform operation, set outputs" flow.
For example:

- a batching cell: the cell pulls its input multiple times, and sets its output with a concatenation of the inputs.
- a patching cell; the cell pulls its input, and sets its output multiple times with a part of the input value.

This is possible in PyPipeline. Example implementations of these two types of cells:

.. code-block:: python

    from typing import Optional, Generic, TypeVar, List
    from pypipeline.cell import ASingleCell, ACompositeCell
    from pypipeline.cellio import Input, Output, RuntimeParameter


    T = TypeVar("T")


    class BatchingCell(ASingleCell, Generic[T]):
        """
        This cell requests X elements, where X is the batch size, and outputs them as a list.
        """

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str):
            super(BatchingCell, self).__init__(parent_cell, name=name)
            self.param_batch_size: RuntimeParameter[int] = RuntimeParameter(self, "batch_size")
            self.input: Input[T] = Input(self, "input")
            self.output_batch: Output[List[T]] = Output(self, "batch")

        def _on_pull(self) -> None:
            batch_size = self.param_batch_size.pull()

            batch: List[T] = []

            for i in range(batch_size):
                value = self.input.pull()
                batch.append(value)

            self.output_batch.set_value(batch)


    class PatchingCell(ASingleCell, Generic[T]):
        """
        This cell requests a list of elements, and outputs them one by one.
        """

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str):
            super(PatchingCell, self).__init__(parent_cell, name=name)
            self.input_batch: Input[List[T]] = Input(self, "batch")
            self.output_value: Output[T] = Output(self, "value")
            self._previous_batch: List[T] = []

        def _on_pull(self) -> None:
            if len(self._previous_batch) == 0:
                self._previous_batch = self.input_batch.pull()

            value = self._previous_batch.pop(0)
            self.output_value.set_value(value)


Important notes on advanced control flow:

1. **Advanced control flow can cause deadlocks (=pulling the pipeline blocks infinitely).**

.. figure:: /data/pypipeline-docs-pulling-gif-pipeline.png

   This is an example of a pipeline that uses the ``BatchingCell`` correctly. The video source will be
   pulled X times by the batching cell, after which the batching cell returns a list of frames.


.. figure:: /data/pypipeline-docs-pulling-gif-pipeline-blocking.png

   This is an example of a pipeline that will cause a deadlock. The batching cell will try to pull
   the video source X times, but the 2nd pull will block, as the source cell has an outgoing connection
   that has not yet pulled. The skip connection will only be pulled when the batch cell is ready, and
   the batch cell will only get ready, when the skip connection gets pulled (X times). Therefore, the
   pipeline is in deadlock.

.. figure:: /data/pypipeline-docs-pulling-gif-pipeline-blocking-2.png

   This is another example of a pipeline that will cause a deadlock. This time the output object of the
   video source cannot be pulled multiple times, without the skip connection being pulled as well.


2. **Scalable cells don't support advanced control flow.**

   The internal cell of a scalable cell must pull its inputs and set its outputs exactly once per pull.
   If not, ex. the internal cell exhibits advanced control flow, the scalable cell will enter a deadlock
   situation when being pulled.

   However, if the internal cell is a pipeline, that pipeline may contain cells that exhibit advanced
   control flow, *as long as the pipeline itself does not*.

.. figure:: /data/pypipeline-docs-pulling-advanced-scalablecell-blocking-3.png

   Example of an invalid pipeline: the internal cell exhibits advanced control flow. The idea of this pipeline
   is as follows: the source provides batches of data, which are unpacked (patched) and packed again (batched)
   such that the prediction cell can predict on single elements at a time. As the patching cell must be pulled
   X times before it will pull its inputs, it will cause a deadlock in the scalable cell.


.. figure:: /data/pypipeline-docs-pulling-advanced-scalablecell-blocking-2.png

   Another example of an invalid pipeline: the internal cell is a pipeline, but this pipeline still exhibits
   advanced control flow.

.. figure:: /data/pypipeline-docs-pulling-advanced-scalablecell.png

   Example of a valid pipeline: the internal cell is a pipeline which contains cells with advanced control
   flow, but the pipeline itself does not. This is allowed, as the patch prediction pipeline on the outside doesn't
   show signs of advanced control flow: it provides a single output for every input.