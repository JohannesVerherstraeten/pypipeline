Topology determination
======================

Source- and sink cells
----------------------

When a pipeline is constructed, PyPipeline will try to determine the topology of its cells.
Determining the topology includes:
 - determining which cells behave as source cells or "data providers",
 - determining which cells behave as sink cells or "data consumers",
 - determining a possible topological ordering or "cell execution order".

Typically, sources will be executed first, and sinks last. Note that a cell can be both a source and a sink at
the same time, for example when your pipeline only consists of one cell.

Directed Acyclic Graphs
-----------------------

The easiest case to do topology determination, is when your cells form a Directed Acyclic Graph (DAG). In this case,
your pipeline doesn't have any cycles.

In a DAG, cells without incoming connections are sources, and cells without outgoing connections are sinks.
A DAG has the property that it has at least one source and one sink.
PyPipeline implements Kahn's algorithm, which is based on this property, to determine all possible topological
orderings of a pipeline and chooses one of the possibilities.

Take for example the following pipeline. It has two sources: Cell1 and Cell5, and two sinks: Cell2 and Cell6.
Multiple topological orderings are possible:
 - Cell1, Cell2, Cell3, Cell4, Cell5, Cell6
 - Cell1, **Cell3**, **Cell2**, Cell4, Cell5, Cell6
 - and many more...

As these topological orderings are equivalent (executing the cells in the different orderings will give the
same result), PyPipeline randomly chooses one of them.

.. figure:: /data/pypipeline-docs-topology-dag.png

   A pipeline forming a Directed Acyclic Graph (DAG).


PyPipeline offers methods to inspect the topology of your pipeline. See the implementation of the example DAG here.
Note that the definitions of the cells itself are left out for readability. The full code can be found **here**. TODO

.. code-block:: python

    class MyPipeline(Pipeline):

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str):
            super(MyPipeline, self).__init__(parent_cell, name=name)
            self.cell1 = Cell1(self, "c1")
            self.cell2 = Cell2(self, "c2")
            self.cell3 = Cell3(self, "c3")
            self.cell4 = Cell4(self, "c4")
            self.cell5 = Cell5(self, "c5")
            self.cell6 = Cell6(self, "c6")

            Connection(self.cell1.output_1, self.cell2.input_1)
            Connection(self.cell1.output_2, self.cell3.input_1)
            Connection(self.cell1.output_2, self.cell4.input_2)
            Connection(self.cell3.output_1, self.cell4.input_1)
            Connection(self.cell3.output_1, self.cell6.input_2)
            Connection(self.cell4.output_1, self.cell6.input_3)
            Connection(self.cell5.output_1, self.cell6.input_1)


    my_pipeline = MyPipeline(None, "my_pipeline")
    my_pipeline.deploy()

    # Inspect the pipeline topology:
    # - Print cells in their topological order
    cells_in_topo_ordering = my_pipeline.get_internal_cells_in_topological_order()
    print([cell.get_name() for cell in cells_in_topo_ordering])

    # - Get a full json description of the topology of your pipeline
    print()
    pprint.pprint(my_pipeline.get_topology_description())       # import pprint (library for pretty printing)

Output:

.. code-block:: python

    ['c1', 'c2', 'c3', 'c4', 'c5', 'c6']

    {'cell': 'MyPipeline("my_pipeline")',
     'internal_cells_topologically_ordered': [{'cell': 'Cell1("c1")', 'is_sink_cell': False, 'is_source_cell': True},
                                              {'cell': 'Cell2("c2")', 'is_sink_cell': True, 'is_source_cell': False},
                                              {'cell': 'Cell3("c3")', 'is_sink_cell': False, 'is_source_cell': False},
                                              {'cell': 'Cell4("c4")', 'is_sink_cell': False, 'is_source_cell': False},
                                              {'cell': 'Cell5("c5")', 'is_sink_cell': False, 'is_source_cell': True},
                                              {'cell': 'Cell6("c6")', 'is_sink_cell': True, 'is_source_cell': False}],
     'internal_connections': [{'connection': 'Connection(Cell1("c1").Output("o1"), Cell2("c2").Input("i1"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False},
                              {'connection': 'Connection(Cell1("c1").Output("o2"), Cell3("c3").Input("i1"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False},
                              {'connection': 'Connection(Cell1("c1").Output("o2"), Cell4("c4").Input("i2"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False},
                              {'connection': 'Connection(Cell3("c3").Output("o1"), Cell4("c4").Input("i1"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False},
                              {'connection': 'Connection(Cell3("c3").Output("o1"), Cell6("c6").Input("i2"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False},
                              {'connection': 'Connection(Cell4("c4").Output("o1"), Cell6("c6").Input("i3"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False},
                              {'connection': 'Connection(Cell5("c5").Output("o1"), Cell6("c6").Input("i1"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False}],
     'is_sink_cell': True,
     'is_source_cell': True}

By chance, the chosen topological ordering is the same as the ordering of definition. Also note that the last two lines
indicate that the cell ``MyPipeline("my_pipeline")`` is both a source and a sink cell, as it has no incoming or
outgoing connections.

Directed Cyclic Graphs (recurrency)
-----------------------------------

The fun starts when we want to determine the topology of pipelines with recurrent connections (cycles). Recurrent
connections occur when the output of a cell should be provided as an input to a previous cell during the next pass.

The following pipeline shows a recurrent connection. In this case, Kahn's algorithm would still work, since the
source still has no incoming connections, and the sink no outgoing ones.

.. figure:: /data/pypipeline-docs-topology-recurrent.png

   A pipeline forming a Directed Cyclic Graph.


Example: Fibonacci pipeline
...........................

The following *Fibonacci* pipeline on the other hand, is not that trivial anymore. This example pipeline generates the
Fibonacci series: ``0, 1, 1, 2, 3, 5, 8, 13, 21, 34,...`` Each number in this series is equal to the sum of the two
previous numbers, starting with 0 and 1.

We define the pipeline as follows:
 - a memory cell, outputting its input of the previous timestep,
 - a Fibonacci cell, outputting the sum of its two input values.

.. figure:: /data/pypipeline-docs-topology-fibonacci.png

   A recurrent pipeline, generating the Fibonacci series.

Let's give it a first try:

.. code-block:: python

    from typing import Optional
    from pypipeline.cell import Pipeline, ASingleCell, ACompositeCell
    from pypipeline.cellio import Input, Output
    from pypipeline.connection import Connection


    class MemoryCell(ASingleCell):

        def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
            super(MemoryCell, self).__init__(parent_cell, name=name)
            self.input: Input[int] = Input(self, "input")
            self.output: Output[int] = Output(self, "output")
            self.prev_input_value: int = None

        def _on_pull(self) -> None:
            result = self.prev_input_value
            self.prev_input_value = self.input.pull()
            self.output.set_value(result)


    class FibonacciCell(ASingleCell):

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str):
            super(FibonacciCell, self).__init__(parent_cell, name=name)
            self.t_minus_one: Input[int] = Input(self, "t_minus_one")
            self.t_minus_two: Input[int] = Input(self, "t_minus_two")
            self.t: Output[int] = Output(self, "t")

        def _on_pull(self) -> None:
            tmin2 = self.t_minus_two.pull()
            tmin1 = self.t_minus_one.pull()
            t = tmin2 + tmin1
            self.t.set_value(t)


    class FibonacciPipeline(Pipeline):

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str) -> None:
            super(FibonacciPipeline, self).__init__(parent_cell, name)

            self.fib: FibonacciCell = FibonacciCell(self, "fib")
            self.mem: MemoryCell[int] = MemoryCell(self, "mem")

            self.c1 = Connection(self.fib.t, self.fib.t_minus_one)          # first recurrent connection
            self.c2 = Connection(self.fib.t, self.mem.input)                # second recurrent connection
            self.c3 = Connection(self.mem.output, self.fib.t_minus_two)


    pipeline = FibonacciPipeline(None, "Fibonacci_pipeline")
    pipeline.deploy()

Output:

.. code-block::

    pypipeline.exceptions.IndeterminableTopologyException:
    FibonacciPipeline("Fibonacci_pipeline"): Could not determine a topological order.
    This is probably caused by a recurrent connection which has no initial_value at
    its start. Make sure you provide an initial value `Output(self, 'output', initial_value=<value>)`
    when creating an output that might provide an input for a next time step.


Woops! PyPipeline could not determine the pipeline topology, and raises an exception. It doesn't know in which order
the cells should be executed, and could therefore not determine which cells are sources/sinks, and which connections
are recurrent or not.

Luckily, the error message gives us a hint towards the solution! We forgot to add the boundary conditions: the fact
that the first two values must be 0 and 1. We should provide one of the initial values at the output where the
recurrent connections start. The other one will be used to initialize the memory cell.

.. code-block:: python
    :emphasize-lines: 7,21

    class MemoryCell(ASingleCell):

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str):
            super(MemoryCell, self).__init__(parent_cell, name=name)
            self.input: Input[int] = Input(self, "input")
            self.output: Output[int] = Output(self, "output")
            self.prev_input_value: int = 0

        def _on_pull(self) -> None:
            result = self.prev_input_value
            self.prev_input_value = self.input.pull()
            self.output.set_value(result)


    class FibonacciCell(ASingleCell):

        def __init__(self, parent_cell: "Optional[ACompositeCell]", name: str):
            super(FibonacciCell, self).__init__(parent_cell, name=name)
            self.t_minus_one: Input[int] = Input(self, "t_minus_one")
            self.t_minus_two: Input[int] = Input(self, "t_minus_two")
            self.t: Output[int] = Output(self, "t", initial_value=1)

        def _on_pull(self) -> None:
            tmin2 = self.t_minus_two.pull()
            tmin1 = self.t_minus_one.pull()
            t = tmin2 + tmin1
            self.t.set_value(t)

    ...

    pipeline = FibonacciPipeline(None, "Fibonacci_pipeline")
    pipeline.deploy()

    for i in range(8):
        pipeline.pull()
        print(pipeline.fib.t.get_value())

Output:

.. code-block:: python

    1
    2
    3
    5
    8
    13
    21
    34


Hurray! Because of the initial value at the output *t*, PyPipeline knows that the two outgoing connections
may be recurrent ones. In that case, the topology is determined, and the pipeline can be executed.

Let's take a look at the topology description output as well. Note that the correct connections are marked as
recurrent. Also note that the memory cell is marked as source (even though it has incoming connections) and
the Fibonacci cell is marked as a sink (even though it has outgoing connections. In this pipeline, they function as
a source and sink respectively. This is also reflected by the topological order: the memory cell needs to be executed
first, before the Fibonacci cell.

.. code-block:: python

    {'cell': 'FibonacciPipeline("Fibonacci_pipeline")',
     'internal_cells_topologically_ordered': [{'cell': 'MemoryCell("mem")', 'is_sink_cell': False, 'is_source_cell': True},
                                              {'cell': 'FibonacciCell("fib")', 'is_sink_cell': True, 'is_source_cell': False}],
     'internal_connections': [{'connection': 'Connection(FibonacciCell("fib").Output("t"), FibonacciCell("fib").Input("t_minus_one"))',
                               'is_inter_cell_connection': True, 'is_recurrent': True},
                              {'connection': 'Connection(FibonacciCell("fib").Output("t"), MemoryCell("mem").Input("input"))',
                               'is_inter_cell_connection': True, 'is_recurrent': True},
                              {'connection': 'Connection(MemoryCell("mem").Output("output"), FibonacciCell("fib").Input("t_minus_two"))',
                               'is_inter_cell_connection': True, 'is_recurrent': False}],
     'is_sink_cell': True,
     'is_source_cell': True}


Explicitly marking connections as (non) recurrent
.................................................

Say that you'd like to really confuse PyPipeline by providing an initial value to the output of the memory cell as
well. In that case, both cells could again be executed first, and the right topology is again not determined anymore.
In this case, PyPipeline will raise the following exception:

.. code-block:: text

    pypipeline.exceptions.IndeterminableTopologyException: FibonacciPipeline("Fibonacci_pipeline"):
    Multiple topological orderings of the internal cells are possible, of which some have different
    recurrent connections. So the ordering in which the internal cells must be executed is undetermined.

    Connections which can be interpreted both recurrent/not-recurrent are:
     * Connection(FibonacciCell("fib").Output("t"), MemoryCell("mem").Input("input"))
     * Connection(MemoryCell("mem").Output("output"), FibonacciCell("fib").Input("t_minus_two")).
    Try adding this parameter to the connection:
    Connection(source, target, explicitly_mark_as_recurrent=<bool>)

Again, the error message gives us a hint on how to solve the issue: we can add a parameter to the connection
to explicitly mark it as recurrent (True) or non-recurrent (False). PyPipeline will prune all possible
topological orders that don't satisfy these requirements. If only one topology remains (or multiple equivalent ones),
the topology is again defined, and the pipeline can be pulled again.