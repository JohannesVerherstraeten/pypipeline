# PyPipeline
# Copyright 2021 Johannes Verherstraeten
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Tuple
import torch
from torch import Tensor
from torch.nn import CrossEntropyLoss
from torchmetrics.functional import accuracy
import torchvision.models.resnet as resnet
import pytorch_lightning as pl

from pathlib import Path
from typing import Optional
from pytorch_lightning.callbacks import ModelCheckpoint

from pypipeline.cell import ASingleCell, ICompositeCell, Pipeline, ScalableCell
from pypipeline.cellio import Input, Output, ConfigParameter, InputPort, OutputPort
from pypipeline.connection.connection import Connection
from pypipeline_lib.torch import CellInputDataLoader, CellInputDataset, DataLoaderSourceCell

from torch.utils.data import DataLoader, random_split
from torchvision.datasets import MNIST
from torchvision.transforms.functional import to_tensor


class ResNetClassifierModule(pl.LightningModule):

    def __init__(self, num_classes: int = 10, num_input_channels: int = 1):
        super(ResNetClassifierModule, self).__init__()
        self.num_classes = num_classes
        self.num_input_channels = num_input_channels
        self.save_hyperparameters()     # saves our __init__ arguments for restoring

        # Configure the model
        self.model = resnet.resnet18(pretrained=False, num_classes=num_classes)
        self.model.conv1 = torch.nn.Conv2d(num_input_channels, 64, kernel_size=7, stride=2, padding=3, bias=False)
        self.final_layer = torch.nn.Softmax(dim=1)

        # Configure loss function
        self.loss_fn = CrossEntropyLoss()

    def forward(self, x: Tensor) -> Tensor:
        x = self.model(x)
        x = self.final_layer(x)
        return x

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=0.001)

    def training_step(self, batch: Tuple[Tensor, Tensor], batch_idx: int):
        x, y = batch
        y_hat = self(x)
        loss = self.loss_fn(y_hat.squeeze(dim=1), y)
        prediction = torch.argmax(y_hat, dim=1)
        acc = accuracy(prediction, y)
        self.log("train_loss", loss)
        self.log("train_acc", acc)
        return loss

    def evaluate(self, batch, stage=None):
        x, y = batch
        y_hat = self(x)
        loss = self.loss_fn(y_hat.squeeze(dim=1), y)
        prediction = torch.argmax(y_hat, dim=1)
        acc = accuracy(prediction, y)
        self.log(f"{stage}_loss", loss, prog_bar=True)
        self.log(f"{stage}_acc", acc, prog_bar=True)

    def validation_step(self, batch: Tuple[Tensor, Tensor], batch_idx: int):
        self.evaluate(batch, "val")

    def test_step(self, batch: Tuple[Tensor, Tensor], batch_idx: int):
        self.evaluate(batch, "test")


class ResNetClassifierTrainingsCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ResNetClassifierTrainingsCell, self).__init__(parent_cell, name)

        # Create inputs and outputs
        self.input_train_image: Input[Tensor] = Input(self, "train_image")
        self.input_train_label: Input[Tensor] = Input(self, "train_label")
        self.input_val_image: Input[Tensor] = Input(self, "val_image")
        self.input_val_label: Input[Tensor] = Input(self, "val_label")
        self.output_model_path: Output[Path] = Output(self, "output_model_path")

        # Create configuration parameters
        # The number of processes is how many times the model must be created in parallel. If use_gpu is on,
        # then an equal amount of GPUs should be available.
        self.config_use_gpu: ConfigParameter[bool] = ConfigParameter(self, "use_gpu")
        self.config_num_processes: ConfigParameter[int] = ConfigParameter(self, "num_processes")
        self.config_batch_size: ConfigParameter[int] = ConfigParameter(self, "batch_size")
        self.config_epochs: ConfigParameter[int] = ConfigParameter(self, "epochs")

        # Only set after cell deployment
        self.classifier: Optional[ResNetClassifierModule] = None

    def _on_deploy(self) -> None:
        super(ResNetClassifierTrainingsCell, self)._on_deploy()
        self.classifier = ResNetClassifierModule(num_classes=10, num_input_channels=1)

    def _on_undeploy(self) -> None:
        super(ResNetClassifierTrainingsCell, self)._on_undeploy()
        self.classifier = None

    def _on_pull(self) -> None:
        assert self.classifier is not None

        # Request the values of our parameters
        batch_size = self.config_batch_size.get_value()
        use_gpu = self.config_use_gpu.get_value()
        num_processes = self.config_num_processes.get_value()
        epochs = self.config_epochs.get_value()

        # Wrap our image and label inputs in a torch.utils.data.Dataset object.
        # PyPipeline has a specific dataset class tailored for this: the CellInputDataset.
        train_dataset = CellInputDataset(self.input_train_image, self.input_train_label)
        train_dataloader = CellInputDataLoader(train_dataset, batch_size=batch_size)

        val_dataset = CellInputDataset(self.input_val_image, self.input_val_label)
        val_dataloader = CellInputDataLoader(val_dataset, batch_size=batch_size)

        # Start the training
        checkpoint_callback = ModelCheckpoint(verbose=False, monitor="val_loss")     # This is very configurable
        if use_gpu:
            trainer = pl.Trainer(max_epochs=epochs, gpus=num_processes,
                                 callbacks=[checkpoint_callback])
        else:
            trainer = pl.Trainer(max_epochs=epochs, num_processes=num_processes, distributed_backend="ddp_cpu",
                                 callbacks=[checkpoint_callback])

        trainer.fit(self.classifier, train_dataloader, val_dataloader)

        # Make the path to the best model checkpoint available as output of the cell.
        best_model_path = Path(checkpoint_callback.best_model_path)
        print(f"Best model path: {best_model_path}")
        self.classifier = self.classifier.load_from_checkpoint(str(best_model_path))
        self.output_model_path.set_value(best_model_path)

    def supports_scaling(self) -> bool:
        # This cell doesn't support parallelizing with PyPipeline, as the training
        # process is stateful.
        return False


class ResNetClassifierTrainingPipeline(Pipeline):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ResNetClassifierTrainingPipeline, self).__init__(parent_cell, name)

        # Create the cells
        self.train_source: DataLoaderSourceCell[Tensor, Tensor] = \
            DataLoaderSourceCell(self, "train_source")
        self.val_source: DataLoaderSourceCell[Tensor, Tensor] = \
            DataLoaderSourceCell(self, "val_source")
        self.classifier = ResNetClassifierTrainingsCell(self, "classifier_training_cell")

        # Create the connections
        Connection(self.train_source.output_sample, self.classifier.input_train_image)
        Connection(self.train_source.output_label, self.classifier.input_train_label)

        Connection(self.val_source.output_sample, self.classifier.input_val_image)
        Connection(self.val_source.output_label, self.classifier.input_val_label)


class ResNetClassifierCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ResNetClassifierCell, self).__init__(parent_cell, name)

        # Create inputs and outputs
        self.input_images: Input[Tensor] = Input(self, "images")
        self.output_labels: Output[Tensor] = Output(self, "labels")

        # Create configuration parameters
        self.config_use_gpu: ConfigParameter[bool] = ConfigParameter(self, "use_gpu")
        self.config_checkpoint: ConfigParameter[Path] = ConfigParameter(self, "checkpoint_to_load")

        # Only set after cell deployment
        self.classifier: Optional[ResNetClassifierModule] = None

    def _on_deploy(self) -> None:
        super(ResNetClassifierCell, self)._on_deploy()
        ckpt_path: Path = self.config_checkpoint.get_value()
        self.classifier = ResNetClassifierModule.load_from_checkpoint(str(ckpt_path))
        self.classifier.eval()

    def _on_undeploy(self) -> None:
        super(ResNetClassifierCell, self)._on_undeploy()
        self.classifier = None

    def _on_pull(self) -> None:
        assert self.classifier is not None

        # Pull the inputs and set the outputs
        images: Tensor = self.input_images.pull()         # Input: shape (batch, channels, x, y), torch.float32, range [0, 1]
        labels = self.classifier(images)  # shape (batch, classes,), torch.float32, range [0, 1]
        labels = labels.cpu().detach()
        self.output_labels.set_value(labels)

    def supports_scaling(self) -> bool:
        return True


class ResNetClassifierScalableCell(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ResNetClassifierScalableCell, self).__init__(parent_cell, name)

        # Create the inputs and outputs
        self.input_images: InputPort[Tensor] = InputPort(self, "images")
        self.output_labels: OutputPort[Tensor] = OutputPort(self, "labels")

        # Create the cells
        self.classifier = ResNetClassifierCell(self, "classifier_cell", )

        # Create the connections
        Connection(self.input_images, self.classifier.input_images)
        Connection(self.classifier.output_labels, self.output_labels)


class ResNetClassifierInferencePipeline(Pipeline):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ResNetClassifierInferencePipeline, self).__init__(parent_cell, name)

        # Create the cells
        # TODO also make a sink cell to store the result?
        # In the inference pipeline, we want to do batch predictions, so don't remove the batch dim:
        self.source: DataLoaderSourceCell[Tensor, Tensor] = DataLoaderSourceCell(self, "source")
        self.classifier_scalable = ResNetClassifierScalableCell(self, "classifier_scalable")

        # Create the connections
        Connection(self.source.output_sample, self.classifier_scalable.input_images)


def test_mnist_training_and_inference():
    # Prepare Dataset and Dataloader to provide to our dataloader source cells.
    mnist_train = MNIST("/home/johannes/data/mnist/", train=True, download=True, transform=to_tensor)
    mnist_train, mnist_val = random_split(mnist_train, [55000, 5000])  # type: ignore
    mnist_test = MNIST("/home/johannes/data/mnist/", train=False, download=True, transform=to_tensor)

    # During training, we want our data to flow one by one through the pipeline, so we use batch size one
    mnist_train_loader = DataLoader(mnist_train, batch_size=1, num_workers=4, shuffle=True)
    mnist_val_loader = DataLoader(mnist_val, batch_size=1, num_workers=4, shuffle=True)
    # But we'll do inference on batches of 4:
    mnist_test_loader = DataLoader(mnist_test, batch_size=4, num_workers=4, shuffle=True)

    # Training pipeline
    # =================
    # Creation
    training_pipeline = ResNetClassifierTrainingPipeline(None, "mnist_training_pipeline")

    # Configuration
    training_pipeline.classifier.config_use_gpu.set_value(True)
    training_pipeline.classifier.config_num_processes.set_value(1)
    training_pipeline.classifier.config_batch_size.set_value(100)
    training_pipeline.classifier.config_epochs.set_value(1)

    training_pipeline.train_source.config_dataloader.set_value(mnist_train_loader)
    training_pipeline.train_source.config_remove_batch_dimension.set_value(True)
    training_pipeline.val_source.config_dataloader.set_value(mnist_val_loader)
    training_pipeline.val_source.config_remove_batch_dimension.set_value(True)

    # Train our model
    training_pipeline.deploy()
    training_pipeline.pull()
    best_checkpoint_path = training_pipeline.classifier.output_model_path.get_value()
    training_pipeline.undeploy()

    # Now that we have trained our classifier, we can use it in an inference pipeline which supports upscaling.
    inference_pipeline = ResNetClassifierInferencePipeline(None, "mnist_inference_pipeline")

    # Configuration
    inference_pipeline.classifier_scalable.classifier.config_use_gpu.set_value(True)
    inference_pipeline.classifier_scalable.classifier.config_checkpoint.set_value(best_checkpoint_path)

    inference_pipeline.source.config_dataloader.set_value(mnist_test_loader)
    inference_pipeline.source.config_remove_batch_dimension.set_value(False)

    # Scale up our scalable cell to 2 parallel instances
    inference_pipeline.classifier_scalable.scale_up(times=2)
    inference_pipeline.deploy()

    assert inference_pipeline.get_nb_available_pulls() > 0

    for i in range(inference_pipeline.get_nb_available_pulls()):
        inference_pipeline.pull()

        # We could add a sink cell that writes the labels to disk or visualizes them together with the input image,
        # but for now we just print them out:
        labels = inference_pipeline.classifier_scalable.output_labels.get_value()
        assert labels.shape[0] == 4     # Batch size 4
        assert labels.shape[1] == 10    # Number of classes

    inference_pipeline.undeploy()
