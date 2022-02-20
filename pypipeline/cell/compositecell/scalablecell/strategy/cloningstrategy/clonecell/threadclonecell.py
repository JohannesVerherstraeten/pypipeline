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

from typing import TYPE_CHECKING

from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ACloneCell
from pypipeline.cell.icellobserver import Event, CloneCreatedEvent
from pypipeline.cellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.connection import Connection
from pypipeline.exceptions import CannotBeDeletedException
from pypipeline.validation import raise_if_not

if TYPE_CHECKING:
    from pypipeline.cell.icell import ICell


class ThreadCloneCell(ACloneCell):
    """
    Since the ScalableCell is implemented such that it executes each clone cell in a separate thread,
    we get multi-threaded clones for free. No threads have to be created here anymore.
    """

    def __init__(self, original_cell: "ICell", name: str):
        """
        Args:
            original_cell: the cell to be cloned.
            name: the name of this thread clone cell.
        Raises:
            InvalidInputException
            NotImplementedError: if the original cell doesn't support cloning.
        """
        super(ThreadCloneCell, self).__init__(original_cell, name)

        self.logger.info(f"  Creating {self}...")

        self._original_cell_clone = original_cell.clone(self)       # TODO may raise exceptions

        for original_cell_clone_input in self._original_cell_clone.get_inputs():
            if isinstance(original_cell_clone_input, IConnectionEntryPoint):
                Connection(self.get_clone_input(original_cell_clone_input.get_name()), original_cell_clone_input)

        for original_cell_clone_output in self._original_cell_clone.get_outputs():
            if isinstance(original_cell_clone_output, IConnectionExitPoint):
                Connection(original_cell_clone_output, self.get_clone_output(original_cell_clone_output.get_name()))

        self.update(CloneCreatedEvent(self))
        self.logger.info(f"  Creating {self} done")

    @classmethod
    def create(cls, original_cell: "ICell", name: str) -> "ThreadCloneCell":
        return ThreadCloneCell(original_cell, name)

    def _on_pull(self) -> None:
        self._original_cell_clone.pull()

    def update(self, event: "Event") -> None:
        super(ThreadCloneCell, self).update(event)
        sync_state = self.get_original_cell()._get_sync_state()
        self._original_cell_clone._set_sync_state(sync_state)

    def _on_reset(self) -> None:
        super(ThreadCloneCell, self)._on_reset()
        self._original_cell_clone.reset()

    def delete(self) -> None:
        raise_if_not(self.can_be_deleted(), CannotBeDeletedException)
        self._original_cell_clone = None        # type: ignore
        super(ThreadCloneCell, self).delete()
