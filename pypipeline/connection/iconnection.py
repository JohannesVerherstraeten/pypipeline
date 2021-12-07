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

from typing import TypeVar, Generic, Dict, Any, Optional, TYPE_CHECKING

from pypipeline.validation import BoolExplained
from pypipeline.cellio import IConnectionEntryPoint, IConnectionExitPoint

if TYPE_CHECKING:
    from pypipeline.cell import ICompositeCell


T = TypeVar('T')


class IConnection(Generic[T]):
    """
    Connection interface.

    An IConnection is owned by its parent cell.

    An IConnection is the controlling class in the IConnection-ICompositeCell relation, as internal connection of the
    composite cell.

    An IConnection is the controlling class in the IConnection-IConnectionEntryPoint relation, as an incoming
    connection of the entry point.

    An IConnection is the controlling class in the IConnection-IConnectionExitPoint relation, as an outgoing
    connection of the exit point.
    """

    def get_source(self) -> "IConnectionExitPoint[T]":
        """
        Returns:
            The source IO of this connection.
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_source(cls, source: "IConnectionExitPoint[T]") -> BoolExplained:
        """
        Args:
            source: the connection exit point to validate.
        Returns:
            TrueExplained if the given connection exit point is a valid source for this connection.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def get_target(self) -> "IConnectionEntryPoint[T]":
        """
        Returns:
            The target IO of this connection.
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_target(cls, target: "IConnectionEntryPoint[T]") -> BoolExplained:
        """
        Args:
            target: the connection entry point to validate.
        Returns:
            TrueExplained if the given connection entry point is a valid target for this connection.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_source_and_target(cls,
                                      source: "IConnectionExitPoint[T]",
                                      target: "IConnectionEntryPoint[T]") -> BoolExplained:
        """
        Args:
            source: the connection exit point to validate.
            target: the connection entry point to validate.
        Returns:
            TrueExplained if this connection can connect the given exit- and entry points in a valid way.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_source_and_target(self) -> None:
        """
        Raises:
            InvalidStateException: the source and target of this connection are invalid.
        """
        raise NotImplementedError

    def get_parent_cell(self) -> "ICompositeCell":
        """
        Returns:
            The parent cell of this connection. (Inside which cell the connection is made)
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_parent_cell(cls, cell: "ICompositeCell") -> BoolExplained:
        """
        Args:
            cell: the cell to validate.
        Returns:
            TrueExplained if the given cell is a valid parent cell for this connection. FalseExplained otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_parent_cell(self) -> None:
        """
        Raises:
            InvalidStateException: if the parent cell of this connection is invalid.
        """
        raise NotImplementedError

    def is_inter_cell_connection(self) -> bool:
        """Connection between 2 cells with the same parent cell"""
        raise NotImplementedError

    def is_intra_cell_connection(self) -> bool:
        """Connection between a composite cell's IO and one of its child cells"""
        raise NotImplementedError

    def is_explicitly_marked_as_recurrent(self) -> bool:
        raise NotImplementedError

    def is_explicitly_marked_as_non_recurrent(self) -> bool:
        raise NotImplementedError

    def is_recurrent(self) -> bool:
        raise NotImplementedError

    def get_topology_description(self) -> Dict[str, Any]:
        raise NotImplementedError

    def assert_has_proper_topology(self) -> None:
        raise NotImplementedError

    def _deploy(self) -> None:
        raise NotImplementedError

    def _undeploy(self) -> None:
        raise NotImplementedError

    def _is_deployed(self) -> bool:
        raise NotImplementedError

    def _assert_is_properly_deployed(self) -> None:
        raise NotImplementedError

    def pull(self) -> T:
        raise NotImplementedError

    def reset(self) -> None:
        raise NotImplementedError

    def get_nb_available_pulls(self) -> Optional[int]:
        raise NotImplementedError

    def assert_is_valid(self) -> None:
        raise NotImplementedError

    def delete(self) -> None:
        raise NotImplementedError
