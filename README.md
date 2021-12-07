# PyPipeline
Encapsulate computations, combine them to algorithms, enable pipeline parallelism and scale up. 

## Installation and documentation
TODO: add link to the documentation.  

## About
**PyPipeline provides a toolbox for building and optimizing the throughput of your data processing pipelines.**

The target usecases of PyPipeline are algorithms that process batches or streams of data. Example usecases are
image- and videostream analysis, financial stock analysis, ... It is specifically tailed for training and inference
of AI-based algorithms, but is in no way limited to the AI domain.

It allows you to:
 * **Build your algorithm** as a pipeline of multiple steps.
 * **Parallelize** (parts of) your pipeline using multithreading and multiprocessing ([Ray](https://ray.io/>)),
   **without having to care about threadsafety or synchronization**. Switching between the two parallelization methods
   is as easy as changing one parameter.
 * **Scale up** parts of your pipeline that are **throughput bottlenecks**. The incoming data is divided over
   multiple clones of these bottlenecks, all working in parallel.
 * **Process your data in order of arrival**, even when upstream parts of your pipeline are parallelized and scaled up.
   (Ordered processing is important if some parts of your pipelines are stateful.)

PyPipeline is built with an emphasis on good object-oriented software design, with as primary purpose making the
code easy to understand, extend and maintain. Its goal is to serve as a solid foundation to deploy and optimize
your applications in production.

PyPipeline is licensed under the 
([GNU Affero General Public License v3](https://www.gnu.org/licenses/agpl-3.0.en.html)). 
It is actively being developed: currently I (Johannes) have 2 days a week reserved for PyPipeline development. 
It is in alpha state, meaning that the interfaces are pretty stable but not yet fixed, and bugs may still occur. 
Please don't hesitate to submit feature requests, file bug reports, ask questions (for now, you can use Github 
Issues for them all). I'll try to answer ASAP. 


## High-level overview
The idea of PyPipeline is to break complex algorithms working on datastreams into multiple pieces called cells.
Each cell is a computational unit with inputs and/or outputs. These inputs and outputs can be connected with each
other, to form a pipeline of cells representing your algorithm. The data is forwarded through the pipeline one by
one or in batch.

On top of this structured approach to algorithm building, PyPipeline adds parallelism and upscaling for free.
Cells can be executed using pipeline parallelism, and bottleneck cells can be duplicated to increase your pipeline's
throughput. The framework handles all locking, synchronization, ordering, queuing and threadsafety for you.

The idea is that cells, inputs, outputs and connections are real objects interacting with
each other. Incoming data arrives at a cell's inputs, the cell performs its operation on it, and
provides the results to its outputs when ready. The connections then route these results to the next cells.

An example usecase for PyPipeline: multiple object tracking in videos:

![testname1](https://github.com/JohannesVerherstraeten/pypipeline/blob/main/docs/source/data/pypipeline-docs-example-tracking-1.png)


The same usecase, but with ScaleUpCells included to allow the scaling up of bottlenecks: 

![testname2](https://github.com/JohannesVerherstraeten/pypipeline/blob/main/docs/source/data/pypipeline-docs-example-tracking-2.png)


## Code example of a dummy pipeline

This example shows a simple pipeline that produces two integers, multiplies them, and prints the result to screen:

![testname3](https://github.com/JohannesVerherstraeten/pypipeline/blob/main/docs/source/data/pypipeline-docs-getting-started-pipeline_new.png)

For more in-depth explanation about this implementation and many more features, see the documentation. 

```python
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


# Execution: 
pipeline = MyPipeline(parent_cell=None, name="my_pipeline")
pipeline.deploy()

for i in range(5):
    pipeline.pull()
```
Output: 
```
0
-1
2
-3
4
```

## Development
Executing mypy for type checking: 
```shell
mypy .
```

Executing all tests: 
```shell
pytest .
```

Executing all tests and determining code coverage: 
```shell
coverage erase
coverage run -m pytest
coverage report -m
coverage html
firefox ./htmlcov/index.html
```

### Code conventions
* Interfaces start with `I` (ex: `ICell`, `IInput`, ...), abstract classes start with `A` (ex: `ACell`). 
  A class can inherit from multiple interfaces, but preferably only inherits from one abstract class. 
* Methods or attributes starting with 1 underscore are protected/package private (in java terms), meaning 
  they should not be called by users. Methods starting with 2 underscores are fully private. 
* Relations between classes are managed very strictly to avoid a pipeline system ending up in an invalid 
  state. TODO elaborate
  

## Stability tests
Next to the pytest tests in the `test/` directory, the following additional setups have been used for 
stability testing. 

For threadsafety testing: 
* `tests/t03_integration_tests/test_08_complex_pipeline.py`, with 1000 iterations, matrix size 3x3 
  and logging level DEBUG (causing thread switches at almost every log line).
  
For memory usage testing: 
* `tests/t03_integration_tests/test_08_complex_pipeline.py`, with 10.000.000 iterations, matrix size 
  of 512x512 and logging level WARNING (to avoid memory buildup of logs). 


### Open tickets (TODO create issues)
- [ ] Installing ray should be optional: implemented, but should still be tested
- [ ] What happens to a FastAPIServer call if somewhere in the pipeline an exception is raised? And what with a headless inference deployment on a production line? And what with a pipeline in development? -> Recover or not?
- [ ] What happens if that exception occurs inside one of the multiple clones of a ScalableCell? -> Recover or not? Implementation with Critical and NonCritical exceptions is ready, but should still be documented.
- [ ] What happens when encoding/decoding a FastAPI input/output fails?
- [ ] Test equally well with parent/clone execution and ray/threading
- [ ] Test deploying scalable cells in different topological orders
- [ ] Check whether all outputs of a cell have been set during pull, otherwise raise an error. -> important to help debugging
- [ ] Get_nb_of_pulls requires cells to be deployed.
- [ ] The reset() backpropagation should happen threadsafe. Example: two scalable cells pulling a source cell, and one of the scalable cells resets while the other is pulling. See also complex pipeline example and recurrency over scalable cells.
- [ ] Implement dataloading cells in a better way.
- [ ] Add dynamic setting of the nb_classes and nb_image_channels in a classifier cell, based on provided dataset -> more elaborate dataset types needed?
- [ ] Enforce scalable cells to have an OutputPort for each output of the clone cell. (test with a clone cell with 1 output, inside a scalable cell without output ports -> CloneOutput semaphore released too many times). Otherwise no connections can be made to that ouptput later on as well.
- [ ] Add a non-blocking run() method to ACell or Pipeline, that checks whether the pipeline is runnable, and if so, continuously runs it in another thread.
- [ ] Set clear explanations bij all asserts and Exceptions
- [ ] Scalable cells: check whether internal cells allow cloning / duplication
- [ ] Scalable cells: check that there are no internal recurrent connections
- [ ] Implement/update/standardize sharing a pipeline between multiple source cells
- [ ] Implement stream-specific pulling and stream-specific parameters.
