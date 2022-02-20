Scalable cells revisited
========================

This page gives some more in-depth information on scalable cells. See the **Getting started > Scale-up basics**
page for more of an introduction.


Implementation design
---------------------

Scalable cells are a type of composite cells. They are able to scale-up their internal cell to
separate concurrent threads and processes, increasing the maximal throughput of the whole pipeline.

Components
..........

The following figure shows the components that are involved when building a pipeline with a scalable
cell.

When deploying the pipeline in the top part of the image (above the dotted line), the scalable
cell will generate clones of the internal cell in the background. It's these clones that will operate
on the data entering the scalable cell. The internal cell (red) will not see any of the data: in fact,
it won't even be deployed to make sure it doesn't acquire any unnecessary resources.

**The internal cell** however still has a function: it can be used as a proxy for its clones. For example,
if you change a (runtime) parameter in the internal cell, the corresponding parameter in all clones
will be synchronized accordingly. This allows you to have control over the cell clones, even when
they run in different threads, processes, or in the future maybe even nodes.

Two types of **clone cells** exist: the ``RayCloneCell`` and ``ThreadCloneCell``. On the outside, they
look exactly the same: they both implement the same interface. Therefore, the scalable cell can treat them
in exactly the same way. This allows to have both types of clones running in parallel at the same time in
the same scalable cell.
On the inside, both ``CloneCells`` also provide the same interface to their internal cell: the pipeline interface.
So the purple cell2 clone living inside the ``CloneCell`` can interact with its parent just as it was a normal
pipeline.

.. figure:: /data/pypipeline-docs-advanced-architecture-scalable-component.png

   Diagram showing the relation between a scalable cell, its internal cell and its clones.


Execution
.........

This section explains what happens during scalable cell execution.

For every one of its clones, the scalable cell will start **a managing thread**.

Every thread will repeatedly:
 1. wait for a column at the left-hand side of the **output queue** (see following figure) to come free
 2. reserve the free column
 3. pull the scalable cell inputs (sequentially)
 4. forward the inputs to the clone (+ if a skip connection is present: put the input value immediately in the output queue (at the reserved column)
 5. perform a ``clone.pull()``
 6. put the outputs of the clone in the output queue (at the reserved column).

All managing threads run concurrently, but only one thread at a time can perform steps 2 and 3. This makes sure
that the results in the output queue always have the same ordering as their corresponding inputs, even when some
of the clones operate significantly faster than the others!

When the scalable cell is pulled, it will wait for the queue column closest to the outputs to become filled
with results. It will move the result values in this column to the outputs, while shifting all values in the
queue one column to the right. This frees up a new column on the left-hand side of the queue, which can
be refilled again by one of the worker threads.


.. figure:: /data/pypipeline-docs-advanced-scalable-cell-description-2.png

   Diagram visualizing the scalable cell output queue and clone threads.


Small note: while the shifting queue is a nice explanation to easily understand what happens, it is not
exactly what happens in reality. In reality, the data in the output queue never gets shifted to the right,
but a **circular queue** is used where the data remains on the same spot, but the pointers to the begin and
the end of the queue move instead.

You can change the size of the result queue by adjusting the preconfigured ``config_queue_capacity``
parameter of the scalable cell. The default queue size is 2. Note that this parameter is a ``ConfigParameter``,
so it needs to be set before deploying.

.. code-block:: python

    scalable_cell = SomeScalableCell(...)
    scalable_cell.config_queue_capacity.set_value(4)


Scaling strategies
------------------
Scalable cells currently have 2 scaling strategies.

The first (and default) one is the **CloningStrategy**:
it allows to clone the internal cell to multiple threads/processes.

The second one is the **NoScalingStrategy**: this one just executes the internal cell in the main thread,
just like a normal pipeline. This can be useful when debugging pipelines with scalable cells.

You can change the scaling strategy of a scalable cell as follows. Note that this must be set before deployment.

.. code-block:: python

    from pypipeline.cell import NoScalingStrategy, CloningStrategy

    scalable_cell = SomeScalableCell(...)
    scalable_cell.set_scaling_strategy_type(CloningStrategy)    # default
    # or
    scalable_cell.set_scaling_strategy_type(NoScalingStrategy)


Note that, when using the NoScalingStrategy, trying to scale up your scalable cell will raise an error.

TODO: does it make sense to use a ``ConfigParameter`` for this?

TODO: in the future we might want to add a strategy that pulls the inputs of the scalable cell concurrently,
instead of sequentially.

Scalable cells vs recurrent connections
---------------------------------------

Scalable cells and recurrent connections have a special relation. To scale up a scalable cell twice or more, it
is usually expected that the internal cell is stateless (= the current output only depends on the current input,
and is fully independent of the previous inputs). This is usually a requirement, as the inputs of the scalable
cell are divided over all clones. On the other hand, recurrent connections introduce state in the system: the
output of a pipeline with recurrent connections doesn't only depend on the input, but also on the previous inputs.
The recurrent connections keep information of the previous inputs alive in the pipeline.

Lets take a closer look at the 3 possible combinations.

**1. A recurrent connection over a scalable cell**. It is easy to build such a pipeline: see image below.
Say that we scale up the scalable cell two times and start to pull it. What would happen?

 1. One of the clone manager threads (thread #1) in the scalable cell start pulling the inputs.
 2. One of the inputs that is being pulled, pulls at the recurrent connection.
 3. The recurrent connection pulls at the output of the scalable cell.
 4. The output has, like any proper output with a recurrent connection, an initial value. This value can be
    returned immediately.
 5. The clone manager thread #1 forwards the input values to its clone for execution.
 6. In the mean time while the clone executes, another clone manager thread #2 starts pulling the inputs.
 7. The recurrent connection is pulled again, pulling the output of the scalable cell as well. This time, the
    initial value has already been seen, and the pull is blocked until a new output value arrives in the output
    queue.
 8. Thread #1 finished the execution of its clone and makes its results available in the output queue.
 9. Only then, thread #2 is unblocked and can start executing its clone.

As you can see, a recurrent connection **kills all parallelism that can possibly happen in the cells that it
passes over**. Even if you scale up multiple times, every clone will only be able to run one at a time. So while
*can* make recurrent connections over scalable cells, it is not recommended.


.. figure:: /data/pypipeline-docs-advanced-scalable-cell-recurrent-1.png

   Example of a pipeline with a recurrent connection over a scalable cell.


**2. A recurrent connection around the internal cell**.

TODO this should not be allowed, but it doesn't raise an error yet.

.. figure:: /data/pypipeline-docs-advanced-scalable-cell-recurrent-2.png

   Example of a pipeline with a recurrent connection around the internal cell of a scalable cell. This is
   an invalid pipeline.

**3. A recurrent connection inside the internal cell**. It is possible to build such a scalable cell (see following figure),
just like it is possible to build a scalable cell with an stateful internal cell. However, note that scaling up
such a cell more than once, will make sure that not every clone sees all data. Therefore, every recurrent connection
will roughly see only 1/X-th of the incoming data, where X is the amount of times the cell is scaled up.
This might have an unwanted impact on your algorithm.

# TODO maybe raise an exception if this happens, but provide a way to suppress the exception if the user knows
what he/she is doing.

.. figure:: /data/pypipeline-docs-advanced-scalable-cell-recurrent-3.png

   Example of a pipeline with a recurrent connection inside the internal cell.


Nesting scalable cells
----------------------

It is possible to nest scalable cells. The example in the following image would produce 2x2=4 clones of the
inner "Cell". However, the inner 2 clones won't run in parallel, as they only get new input data from the
outer clone when they provided the result for the previous input data. As a consequence, the throughput is
exactly the same as without the inner scalable cell being scaled up. Therefore it is not recommended to nest
scalable cells, as they may needlessly require resources without providing benefit.

.. figure:: /data/pypipeline-docs-advanced-scalable-cell-nested-1.png

   Example of a pipeline with nested scalable cells.


Scaling up and down at runtime
------------------------------

It is possible to scale up and down scalable cells while they are deployed an running. Note that scaling down
a running scalable cell may lead to the "loss" of a few data elements of your input stream. This can happen
when a clone is removed that was still busy processing its input data.
