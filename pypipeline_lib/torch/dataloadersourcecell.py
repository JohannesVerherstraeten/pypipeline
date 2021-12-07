# Copyright (C) 2021  Johannes Verherstraeten
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see https://www.gnu.org/licenses/agpl-3.0.en.html

from typing import Optional, Tuple, Iterator, Generic, TypeVar
from torch.utils.data.dataloader import DataLoader

from pypipeline.cell import ASingleCell, ICompositeCell
from pypipeline.cellio import Output, ConfigParameter


T = TypeVar("T")
SampleT = TypeVar("SampleT")
LabelT = TypeVar("LabelT")


class ADataLoaderSourceCell(ASingleCell, Generic[T]):
    """
    Abstract source cell for pytorch DataLoaders.
    """

    def __init__(self,
                 parent_cell: "Optional[ICompositeCell]",
                 name: str):
        super(ADataLoaderSourceCell, self).__init__(parent_cell, name=name)

        self.__dataloader: Optional["DataLoader[T]"] = None
        self.__dataloaderiter: Optional[Iterator[T]] = None

        self.config_dataloader: ConfigParameter[DataLoader[T]] = ConfigParameter(self, "dataloader")

    def _get_dataloader(self) -> Optional["DataLoader[T]"]:
        return self.__dataloader

    def _set_dataloader(self, dataloader: Optional["DataLoader[T]"]) -> None:
        self.__dataloader = dataloader
        self.__dataloaderiter = None

    def _on_reset(self) -> None:
        super(ADataLoaderSourceCell, self)._on_reset()
        self.__dataloaderiter = None

    def _on_deploy(self) -> None:
        super(ADataLoaderSourceCell, self)._on_deploy()
        dataloader: DataLoader[T] = self.config_dataloader.get_value()
        self._set_dataloader(dataloader)

    def _on_undeploy(self) -> None:
        super(ADataLoaderSourceCell, self)._on_undeploy()
        self._set_dataloader(None)

    def _set_outputs(self, items: T) -> None:
        """
        This method is called for each item or set of items that come out of the dataloader.
        Any subclass must override this method to set its outputs with the items.
        """
        raise NotImplementedError

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        """
        Raises StopIteration when exhausted.
        """
        dataloader = self._get_dataloader()
        assert dataloader is not None

        # TODO use self.deploy() for this?
        if self.__dataloaderiter is None:
            self.__dataloaderiter = iter(dataloader)

        items: T = next(self.__dataloaderiter)      # May raise StopIteration
        self._set_outputs(items)

    def get_nb_available_pulls(self) -> Optional[int]:
        dataloader = self._get_dataloader()
        if dataloader is None:
            raise Exception(f"{self}: no dataloader available. "
                            f"Please provide a dataloader to this cell: `cell.set_dataloader(dl)`")
        return len(dataloader)


class DataLoaderSourceCell(ADataLoaderSourceCell[Tuple[SampleT, LabelT]], Generic[SampleT, LabelT]):
    """
    Source cell for pytorch DataLoaders loading sample-label pairs.

    This cell inherits the configuration parameter:

    self.config_dataloader: ConfigParameter[DataLoader[Tuple[SampleT, LabelT]]] = ConfigParameter(self, "dataloader")

    -> don't forget to set it before deploying.
    """

    def __init__(self,
                 parent_cell: "Optional[ICompositeCell]",
                 name: str):     # If True, the batch size of the dataloader must be 1
        super(DataLoaderSourceCell, self).__init__(parent_cell, name)

        self.__remove_batch_dim: Optional[bool] = None

        self.output_sample: Output[SampleT] = Output(self, "sample")
        self.output_label: Output[LabelT] = Output(self, "label")

        self.config_remove_batch_dimension: ConfigParameter[bool] = ConfigParameter(self, "remove_batch_dim")
        self.config_remove_batch_dimension.set_value(False)     # Provide a default value

    def _on_deploy(self) -> None:
        super(DataLoaderSourceCell, self)._on_deploy()
        self.__remove_batch_dim = self.config_remove_batch_dimension.get_value()
        if self.__remove_batch_dim and self._get_dataloader().batch_size != 1:
            raise ValueError(f"f{self} can only remove the batch dimension if the dataloader loads batches of size 1.")

    def _on_undeploy(self) -> None:
        super(ADataLoaderSourceCell, self)._on_undeploy()
        self.__remove_batch_dim = None

    def _set_outputs(self, items: Tuple[SampleT, LabelT]) -> None:
        if not isinstance(items, (tuple, list)) or len(items) != 2:
            raise ValueError(f"Expected the dataloader configured at {self}.config_dataloader to provide 2 items: "
                             f"a sample and a label. \nGot: {items}")
        assert self.__remove_batch_dim is not None
        sample, label = items
        if self.__remove_batch_dim:
            sample = sample[0]
            label = label[0]
        self.output_sample.set_value(sample)
        self.output_label.set_value(label)


if __name__ == '__main__':
    """
    Example usage. 
    """
    from torch import Tensor
    from torch.utils.data import DataLoader, random_split
    from torchvision.datasets import MNIST
    from torchvision.transforms.functional import to_tensor

    # Prepare Dataset and Dataloader to provide to our dataloader source cells.
    mnist_train = MNIST("/home/johannes/data/mnist/", train=True, download=True, transform=to_tensor)
    mnist_train, mnist_val = random_split(mnist_train, [55000, 5000])       # type: ignore
    mnist_test = MNIST("/home/johannes/data/mnist/", train=False, download=True, transform=to_tensor)

    # Our data flows one by one through the pipeline, so we must use batch size one
    mnist_train_loader = DataLoader(mnist_train, batch_size=1, num_workers=4, shuffle=True)
    mnist_val_loader = DataLoader(mnist_val, batch_size=1, num_workers=4, shuffle=True)
    # Except during inference, where we will do batch-wise inference (batch size 4)
    mnist_test_loader = DataLoader(mnist_test, batch_size=4, num_workers=4, shuffle=True)

    # We just make a single source cell as example, but you can just as well embed this cell in your pipeline.
    source: DataLoaderSourceCell[Tensor, Tensor] = DataLoaderSourceCell(None, "source")
    source.config_dataloader.set_value(mnist_train_loader)
    source.config_remove_batch_dimension.set_value(True)

    source.deploy()
    print(f"Available pulls: {source.get_nb_available_pulls()}")
    source.pull()
    print(source.output_sample.get_value().shape)
    print(source.output_label.get_value())

    source.undeploy()
    source.config_dataloader.set_value(mnist_test_loader)
    source.config_remove_batch_dimension.set_value(False)

    source.deploy()
    print(f"Available pulls: {source.get_nb_available_pulls()}")
    source.pull()
    print(source.output_sample.get_value().shape)
    print(source.output_label.get_value())