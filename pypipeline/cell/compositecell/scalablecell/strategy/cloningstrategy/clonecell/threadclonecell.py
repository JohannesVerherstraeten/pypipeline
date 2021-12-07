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
        """
        Factory method to create a new clone.

        Args:
            original_cell: the original cell to be cloned.
            name: the name of the new clone cell.
        Returns:
            A new clone cell.
        Raises:
            InvalidInputException
            NotImplementedError: if the original cell doesn't support cloning.
        """
        return ThreadCloneCell(original_cell, name)

    def _on_pull(self) -> None:
        """
        Override this method to add functionality that must happen when pulling the cell.

        During a pull, a cell must pull its inputs, execute it's functionality and set its outputs.

        Raises:
            Exception: any exception that the user may raise when overriding _on_pull.

        Won't raise:
            NotDeployedException: this method will only be called when the cell is already deployed.
            IndeterminableTopologyException: this method will only be called when the cell is already deployed.
        """
        self._original_cell_clone.pull()

    def update(self, event: "Event") -> None:
        """
        Will be called by the observables of this observer (ex: internal cell).

        What should trigger this update:
        - a parameter of the observed cell changes,

        Args:
            event: the event to notify this observer about.
        """
        super(ThreadCloneCell, self).update(event)
        sync_state = self.get_original_cell()._get_sync_state()
        self._original_cell_clone._set_sync_state(sync_state)

    def _on_reset(self) -> None:
        """
        Override this method to add functionality that must happen when resetting the cell.

        During a reset, a cell must clear its internal state. This doesn't include cell configuration, but only
        cell state that was accumulated during consecutive pulls.
        Ex: reset the dataloader iterator of a dataloader cell.
        Ex: the currently accumulated batch in a BatchingCell.

        Don't forget to call the _on_reset of the super-class when overriding this method! Ex:
        ```
        def _on_reset(self) -> None:
            super(MyCell, self)._on_reset()
            # other resetting code
        ```

        Raises:
            Exception: any exception that the user may raise when overriding _on_reset.
        """
        super(ThreadCloneCell, self)._on_reset()
        self._original_cell_clone.reset()

    def delete(self) -> None:
        """
        Deletes this cell, and all its internals.

        Raises:
            CannotBeDeletedException
        """
        raise_if_not(self.can_be_deleted(), CannotBeDeletedException)
        self._original_cell_clone = None        # type: ignore
        super(ThreadCloneCell, self).delete()
