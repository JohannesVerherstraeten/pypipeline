Neural network training
=======================

PyPipeline will provide tools for easily integrating with other libraries in the ``pypipeline_lib`` package.

Currently, tools are available to interact easily with the PyTorch library for deep learning.
 - A source cell that fetches its data from a PyTorch DataLoader.
 - PyTorch Dataset and DataLoader objects that fetch their data from a pipeline.
 - A control flow system to handle pipeline resets and data reshuffling between training epochs, etc...


Source cell fetching data from a PyTorch DataLoader
---------------------------------------------------
The ``pypipeline_lib.torch.DataLoaderSourceCell`` is a cell that can be configured with a PyTorch DataLoader.
The dataloader should at each iteration provide a (sample, label) pair, which the source cell will set
at its corresponding outputs.

.. figure:: /data/pypipeline-docs-neural-networks-dataloadersourcecell.png

   A DataLoaderSourceCell.

When the dataloader has batch size 1, the source cell can be configured to remove the batch dimension from the sample
and label (=stream-wise dataloading).

Example of stream-wise dataloading:

.. code-block:: python

    from torch import Tensor
    from torch.utils.data import DataLoader, random_split
    from torchvision.datasets import MNIST
    from torchvision.transforms.functional import to_tensor

    # Prepare Dataset and Dataloader to provide to our dataloader source cells.
    mnist_train = MNIST("mnist/", train=True, download=True, transform=to_tensor)
    mnist_test = MNIST("mnist/", train=False, download=True, transform=to_tensor)

    # Our data flows one by one through the pipeline during training, so we must use batch size one
    mnist_train_loader = DataLoader(mnist_train, batch_size=1, num_workers=4, shuffle=True)

    # We just make a single source cell as example, but you can just as well embed this cell in your pipeline.
    source: DataLoaderSourceCell[Tensor, Tensor] = DataLoaderSourceCell(None, "source")
    source.config_dataloader.set_value(mnist_train_loader)
    # As our data flows one by one through the pipeline, we want to remove the batch dimension:
    source.config_remove_batch_dimension.set_value(True)

    source.deploy()
    print(f"Available pulls: {source.get_nb_available_pulls()}")
    source.pull()
    print(source.output_sample.get_value().shape)
    print(source.output_label.get_value())

Output:

.. code-block:: text

    Available pulls: 60000
    torch.Size([1, 28, 28])
    tensor(8)

Example of batch-wise dataloading:

.. code-block:: python

    mnist_test_loader = DataLoader(mnist_test, batch_size=4, num_workers=4, shuffle=True)

    source.config_dataloader.set_value(mnist_test_loader)
    source.config_remove_batch_dimension.set_value(False)       # Default is False, so this line isn't strictly necessary

    source.deploy()
    print(f"Available pulls: {source.get_nb_available_pulls()}")
    source.pull()
    print(source.output_sample.get_value().shape)
    print(source.output_label.get_value())

Output:

.. code-block:: text

    Available pulls: 2500
    torch.Size([4, 1, 28, 28])
    tensor([0, 7, 8, 0])


PyTorch Dataset and DataLoader fetching data from a pipeline
------------------------------------------------------------

When you train models in PyTorch, you usually create a ``Dataset`` and ``DataLoader`` to efficiently provide
data to your model.

Imagine that you have build up a pipeline with a few heavy preprocessing steps, followed by a model to be trained.
The model cell can pull its inputs to fetch data from the pipeline and provide it to the model training code,
but we would like to have this data fetching done with the same interface as the PyTorch dataset. (This can
for example be useful to integrate with model training frameworks like PyTorch Lightning.)

PyPipeline provides implementations of the PyTorch dataset and dataloader interfaces in the ``pypipeline_lib``
package: ``pypipeline_lib.torch.CellInputDataset`` and ``pypipeline_lib.torch.CellInputDataLoader``.
You initialize the CellInputDataset with the inputs from which it should fetch its data, and at every iteration,
it will pull each input once and provide the input values as a tuple or list.

If your source cells implement the ``get_nb_available_pulls()`` method, you can even request the ``len()`` of
your dataset or dataloader.

See the following code as example. Note that it uses a form of advanced control flow: during one pull of the TestCell,
the TestCell inputs get pulled until the source raises a ``StopIteration``.


.. code-block:: python
   :emphasize-lines: 35,36

    from pypipeline.cell import ASingleCell, Pipeline
    from pypipeline.cellio import Input, Output
    from pypipeline.connection import Connection
    from pypipeline_lib.torch import CellInputDataset, CellInputDataLoader


    class TestSource(ASingleCell):

        def __init__(self, parent):
            super(TestSource, self).__init__(parent, "testsource")
            self.output_x = Output(self, "x")
            self.output_y = Output(self, "y")
            self.counter = 0
            self.counter_max = 10

        def _on_pull(self) -> None:
            if self.counter >= self.counter_max:
                raise StopIteration
            self.output_x.set_value(self.counter)
            self.output_y.set_value(self.counter * 2)
            self.counter += 1

        def get_nb_available_pulls(self) -> Optional[int]:
            return self.counter_max


    class TestCell(ASingleCell):

        def __init__(self, parent):
            super(TestCell, self).__init__(parent, "testcell")
            self.input_x = Input(self, "x")
            self.input_y = Input(self, "y")

        def _on_pull(self) -> None:
            dataset = CellInputDataset(self.input_x, self.input_y)
            dataloader = CellInputDataLoader(dataset, batch_size=3)

            print(f"Number of available CellInputDataLoader iterations: {len(dataloader)}")
            for pair in dataloader:
                print(pair)


    class TestPipeline(Pipeline):

        def __init__(self):
            super(TestPipeline, self).__init__(None, "testpipeline")
            self.source = TestSource(self)
            self.cell = TestCell(self)
            Connection(self.source.output_x, self.cell.input_x)
            Connection(self.source.output_y, self.cell.input_y)

    p = TestPipeline()
    p.deploy()
    p.pull()

Output:

.. code-block:: text

    Number of available CellInputDataLoader iterations: 4
    [tensor([0, 1, 2]), tensor([0, 2, 4])]
    [tensor([3, 4, 5]), tensor([ 6,  8, 10])]
    [tensor([6, 7, 8]), tensor([12, 14, 16])]
    [tensor([9]), tensor([18])]


Pipeline reset before each epoch
--------------------------------

Training a neural network model usually takes multiple iterations (epochs) through your train dataset. This means that,
at the start of every new epoch, your pipeline needs to be reset: the source cells should start loading their data from
the start again (possibly with a data reshuffle), eventual stateful preprocessing steps should clear their state, etc...

PyPipeline provides the mechanics for this cell reset: just like a ``cell.pull()`` call propagates upstream until it
reaches the source cells, a ``cell.reset()`` propagates and resets all upstream cells too.

When using the CellInputDataset and CellInputDataLoader objects, this reset is automatically called when starting a
new iteration on your dataloader.

The following example extends the previous example with the reset functionality. Note that the source cells
(or any stateful cell!) should override implement the ``reset()`` method. If you use the ``DataLoaderSourceCell``
as source cell in your pipeline, this reset method is already overridden.

.. note::
   When overriding the ``reset()`` method, don't forget to call the reset of the superclass!
   ``super(YourCell, self).reset()``

.. code-block:: python
   :emphasize-lines: 21,22,23,24,41,42,43,44

    from pypipeline.cell import ASingleCell, Pipeline
    from pypipeline.cellio import Input, Output
    from pypipeline.connection import Connection

    class TestSource(ASingleCell):

        def __init__(self, parent):
            super(TestSource, self).__init__(parent, "testsource")
            self.output_x = Output(self, "x")
            self.output_y = Output(self, "y")
            self.counter = 0
            self.counter_max = 10

        def _on_pull(self) -> None:
            if self.counter >= self.counter_max:
                raise StopIteration
            self.output_x.set_value(self.counter)
            self.output_y.set_value(self.counter * 2)
            self.counter += 1

        def _on_reset(self) -> None:
            super(TestSource, self)._on_reset()
            print(f"{self} is reset!")
            self.counter = 0

        def get_nb_available_pulls(self) -> Optional[int]:
            return self.counter_max

    class TestCell(ASingleCell):

        def __init__(self, parent):
            super(TestCell, self).__init__(parent, "testcell")
            self.input_x = Input(self, "x")
            self.input_y = Input(self, "y")

        def _on_pull(self) -> None:
            ds = CellInputDataset(self.input_x, self.input_y)
            dl = CellInputDataLoader(ds, batch_size=3)

            print(f"Number of available CellInputDataLoader iterations: {len(dl)}")
            for epoch in range(2):
                print(f"Epoch {epoch}:")
                for pair in dl:
                    print(pair)

    class TestPipeline(Pipeline):

        def __init__(self):
            super(TestPipeline, self).__init__(None, "testpipeline")
            self.source = TestSource(self)
            self.cell = TestCell(self)
            Connection(self.source.output_x, self.cell.input_x)
            Connection(self.source.output_y, self.cell.input_y)

    p = TestPipeline()
    p.deploy()
    p.pull()

Output: (TODO solve the reset() being called for every output the cell contains. It has no negative side effects, but
is not super clean)

.. code-block:: text

    Number of available CellInputDataLoader iterations: 4
    Epoch 0:
    TestSource("testsource") is reset!
    TestSource("testsource") is reset!
    [tensor([0, 1, 2]), tensor([0, 2, 4])]
    [tensor([3, 4, 5]), tensor([ 6,  8, 10])]
    [tensor([6, 7, 8]), tensor([12, 14, 16])]
    [tensor([9]), tensor([18])]
    Epoch 1:
    TestSource("testsource") is reset!
    TestSource("testsource") is reset!
    [tensor([0, 1, 2]), tensor([0, 2, 4])]
    [tensor([3, 4, 5]), tensor([ 6,  8, 10])]
    [tensor([6, 7, 8]), tensor([12, 14, 16])]
    [tensor([9]), tensor([18])]


Examples
--------

1. MNIST classification example
...............................

`Link to the notebook <https://github.com/JohannesVerherstraeten/pypipeline/blob/main/examples/mnistclassification/mnistclassification.ipynb>`_.

Visualization of the MNIST example pipelines:

.. figure:: /data/mnistclassification-training-pipeline.png

   MNIST classification training pipeline


.. figure:: /data/mnistclassification-inference-pipeline.png

   MNIST classification (scalable) inference pipeline
