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

from typing import Set, TYPE_CHECKING, TypeVar, Generic, Any, Callable, List, Optional, Iterator
from torch.utils.data import DataLoader, IterableDataset

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.cellio import Input


T = TypeVar("T")


# Should be IterableDataset[T], but not allowed in pytorch 1.6? TODO check
class CellInputDataset(IterableDataset[T], Generic[T]):

    def __init__(self, *inputs: "Input", transform: Optional[Callable[[Any], Any]] = None):
        """
        Pulls the given inputs once during a __getitem__, and performs the optional transform to each item
        before returning them as a tuple.
        """
        super(CellInputDataset, self).__init__()
        self.inputs = inputs
        assert len(self.inputs) > 0
        self.transform = transform
        cells: Set[ICell] = {input_.get_cell() for input_ in self.inputs}
        assert len(cells) == 1, "Cannot create a dataset from inputs from multiple cells (yet?)"

    def get_next(self) -> T:
        result = [inp.pull() for inp in self.inputs]
        if self.transform is not None:
            result = [self.transform(item) for item in result]
        if len(self.inputs) == 1:
            return result[0]    # type: ignore
        return tuple(result)    # type: ignore

    def get_nb_available_pulls(self) -> Optional[int]:
        available_pulls_per_input = [inp.get_nb_available_pulls() for inp in self.inputs]
        available_pulls_per_input_not_none = [nb for nb in available_pulls_per_input if nb is not None]
        if len(available_pulls_per_input_not_none) == 0:
            return None
        return min(available_pulls_per_input_not_none)

    def has_fixed_length(self) -> bool:
        available_pulls = self.get_nb_available_pulls()
        return available_pulls is not None

    def __len__(self) -> int:
        available_pulls = self.get_nb_available_pulls()
        if available_pulls is None:
            # When not available, __len__ must raise a TypeError to be compatible with the PyTorch DataLoader
            raise TypeError(f"Length of a CellInputDataset is not defined when the nb of "
                            f"available pulls of the pipeline is unknown. \n"
                            f"If you do know how many pulls a pipeline should be able to do, "
                            f"override the get_nb_available_pulls() in your pipeline's source "
                            f"cells. ")
        return available_pulls

    def __getitem__(self, index) -> T:
        return next(self)

    def __iter__(self):
        return self

    def __next__(self) -> T:
        return self.get_next()

    def reset(self) -> None:
        for input_ in self.inputs:
            input_.reset()


# Should be DataLoader[T], but not allowed TODO check. + add arguments
class CellInputDataLoader(DataLoader, Generic[T]):

    def __init__(self,          # type: ignore
                 dataset: CellInputDataset[T],
                 batch_size: int = 1,
                 collate_fn: Optional[Callable[[List[T]], T]] = None,
                 pin_memory: bool = False,
                 timeout: float = 0.,
                 worker_init_fn: Optional[Callable[[int], None]] = None,
                 drop_last: bool = False,
                 **kwargs):
        assert "shuffle" not in kwargs or not kwargs["shuffle"], \
            "Shuffling is not allowed, as the CellInputDataset doesn't have control over the source cell's " \
            "data injection order."
        assert "num_workers" not in kwargs or kwargs["num_workers"] == 0, \
            "If you want to parallelize data loading of pipeline, use ScaleUpCells or use parallel data loading in " \
            "your source cell(s). Pulling a CellInputDataset from multiple workers is not allowed. "
        super(CellInputDataLoader, self).__init__(dataset, batch_size=batch_size,
                                                  collate_fn=collate_fn, pin_memory=pin_memory,     # type: ignore
                                                  timeout=timeout, worker_init_fn=worker_init_fn,
                                                  drop_last=drop_last)   # type: ignore

    def __iter__(self) -> Iterator[T]:      # type: ignore
        # assert isinstance(self.dataset, _DatasetDecorator)
        assert isinstance(self.dataset, CellInputDataset)
        self.dataset.reset()
        return super(CellInputDataLoader, self).__iter__()


if __name__ == '__main__':
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

        def pull(self) -> None:
            if self.counter >= self.counter_max:
                raise StopIteration
            self.output_x.set_value(self.counter)
            self.output_y.set_value(self.counter * 2)
            self.counter += 1

        def reset(self) -> None:
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

        def pull(self) -> None:
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
