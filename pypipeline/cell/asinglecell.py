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

from typing import Optional, TYPE_CHECKING
from abc import abstractmethod

from pypipeline.cell.acell import ACell

if TYPE_CHECKING:
    from pypipeline.cell.compositecell.icompositecell import ICompositeCell


class ASingleCell(ACell):
    """
    All methods denoted here can/should be overridden when creating a new cell.
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        """
        Args:
            parent_cell: the cell in which this cell must be nested.
            name: name of this cell.
        Raises:
            InvalidInputException
        """
        super(ASingleCell, self).__init__(parent_cell, name=name)

    def _on_deploy(self) -> None:
        """
        Override this method to add functionality that must happen when deploying the cell.

        Don't forget to call the _on_deploy of the super-class when overriding this method! Ex:
        ```
        def _on_deploy(self) -> None:
            super(MyCell, self)._on_deploy()
            # other deployment code
        ```

        Raises:
            Exception: any exception that the user may raise when overriding _on_deploy.
        """
        super(ASingleCell, self)._on_deploy()

    def _on_undeploy(self) -> None:
        """
        Override this method to add functionality that must happen when undeploying the cell.

        Don't forget to call the _on_undeploy of the super-class when overriding this method! Ex:
        ```
        def _on_undeploy(self) -> None:
            super(MyCell, self)._on_undeploy()
            # other undeployment code
        ```

        Raises:
            Exception: any exception that the user may raise when overriding _on_undeploy.
        """
        super(ASingleCell, self)._on_undeploy()

    @abstractmethod
    def _on_pull(self) -> None:
        """
        Override this method to add functionality that must happen when pulling the cell.

        During a pull, a cell must pull its inputs, execute it's functionality and set its outputs.

        Raises:
            Exception: any exception that the user may raise when overriding _on_pull.
        """
        raise NotImplementedError

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
        super(ASingleCell, self)._on_reset()

    def supports_scaling(self) -> bool:
        """
        Override this method to indicate that the cell supports scaling up more than once.

        A cell support scaling if it supports multiple clones of it running in parallel. This should usually be False
        for stateful cells.

        Returns:
            True if this cell supports being scaled up more than once. False otherwise.
        """
        raise NotImplementedError

    def get_nb_required_gpus(self) -> float:
        """
        Override this method to indicate how much GPUs your cell needs.

        Note: very experimental feature - might not yet work at all.

        Returns:
            The number of GPUs this cell needs. Can be a fraction.
        """
        return 0

    def get_nb_available_pulls(self) -> Optional[int]:
        """
        Returns the total number of times this cell can be pulled.

        Override this method when implementing a source cell.

        Default implementation here is only valid if the cell pulls all its inputs once per cell pull. If this is
        not the case, the cell must override this method. Also when a cell has no inputs, it has to override this
        method itself.

        Returns:
            The total number of times this cell can be pulled, or
            None if cell can be pulled infinitely or an unspecified amount of time.
        """
        return super(ASingleCell, self).get_nb_available_pulls()
