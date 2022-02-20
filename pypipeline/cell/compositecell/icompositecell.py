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

from typing import Optional, TYPE_CHECKING, Sequence

from pypipeline.cell.icell import ICell
from pypipeline.cell.icellobserver import IObserver

if TYPE_CHECKING:
    from pypipeline.cell.compositecell.topology import Topology
    from pypipeline.connection import IConnection
    from pypipeline.validation import BoolExplained


class ICompositeCell(ICell, IObserver):
    """
    Composite cell interface.

    An ICell is owned by its (optional) parent cell.
    An ICell owns its IO.
    An ICompositeCell owns its internal cells.
    An ICompositeCell owns its internal connections.

    An ICell is the controlled class in the ICompositeCell-ICell relation, as internal cell of a parent composite class.
    An ICompositeCell is the controlling class in the ICompositeCell-ICell relation, as parent of internal cells.

    An ICell is the controlled class in the IObserver-IObservable relation, as observable.
    An ICompositeCell is the controlling class in the IObserver-IObservable relation, as potential observer of internal cells.

    An ICell is the controlled class in the IO-ICell relation, as owner of the IO.

    An ICompositeCell is the controlled class in the IConnection-ICompositeCell relation, as the parent cell of the
    connection.
    """

    # ------ Internal cells ------

    def get_internal_cells(self) -> "Sequence[ICell]":
        """
        Returns:
            The internal cells of this composite cell (non-recursively).
        """
        raise NotImplementedError

    def get_internal_cells_recursively(self) -> Sequence["ICell"]:
        """
        Returns:
            The internal cells of this composite cell recursively.
        """
        raise NotImplementedError

    def get_internal_cell_names(self) -> Sequence[str]:
        """
        Returns:
            The names of the internal cells of this composite cell (non-recursively).
        """
        raise NotImplementedError

    def get_internal_cell_with_name(self, name: str) -> "ICell":
        """
        Args:
            name: the name of an internal cell.
        Returns:
            The internal cell with the given name.
        Raises:
            KeyError: if no internal cell with the given name exists.
        """
        raise NotImplementedError

    def _add_internal_cell(self, cell: "ICell") -> None:
        """
        Should only be used by ICell instances in their __init__.

        Main mutator in the ICompositeCell-ICell relation, as parent of internal cells.
        Main mutator in the IObserver-IObservable relation, as potential observer of internal cells.

        Args:
            cell: the internal cell to add to this composite cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
        raise NotImplementedError

    def _remove_internal_cell(self, cell: "ICell") -> None:
        """
        Should only be used by ICell instances in their delete().

        Main mutator in the ICompositeCell-ICell relation, as parent of internal cells.
        Main mutator in the IObserver-IObservable relation, if this relation is required.

        Args:
            cell: the internal cell to remove from this composite cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
        raise NotImplementedError

    def can_have_as_internal_cell(self, cell: "ICell") -> "BoolExplained":
        """
        Args:
            cell: internal cell to validate.
        Returns:
            TrueExplained if the given cell is a valid internal cell for this composite cell. FalseExplained otherwise.
        """
        raise NotImplementedError

    def get_nb_internal_cells(self) -> int:
        """
        Returns:
            The number of internal cells inside this composite cell (non-recursively).
        """
        raise NotImplementedError

    def get_max_nb_internal_cells(self) -> int:
        """
        Returns:
            The maximum amount of internal cells that can be nested inside this composite cell.
        """
        raise NotImplementedError

    def can_have_as_nb_internal_cells(self, number_of_internal_cells: int) -> "BoolExplained":
        """
        Main validator in the ICell-ICompositeCell relation, as parent of internal cells.

        Args:
            number_of_internal_cells: the number to validate.
        Returns:
            TrueExplained if the given number of internal cells is allowed. FalseExplained otherwise.
        """
        raise NotImplementedError

    def has_as_internal_cell(self, cell: "ICell") -> bool:
        """
        Args:
            cell: a cell object
        Returns:
            True if the given cell is an internal cell of this composite cell. False otherwise.
        """
        raise NotImplementedError

    def has_as_internal_cell_name(self, name: str) -> bool:
        """
        Args:
            name: a cell name
        Returns:
            True if this composite cell has an internal cell with the given name. False otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_internal_cells(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the internal cells is invalid.
        """
        raise NotImplementedError

    # ------ Internal connections ------

    def _add_internal_connection(self, connection: "IConnection") -> None:
        """
        Should only be used by IConnection instances in their __init__.

        Auxiliary mutator in the IConnection-ICompositeCell relation, as the parent cell of the connection.

        Args:
            connection: the internal connection to add.
        Raises:
            AlreadyDeployedException: if this cell is already deployed
            InvalidInputException
        """
        raise NotImplementedError

    def _remove_internal_connection(self, connection: "IConnection") -> None:
        """
        Should only be used by IConnection instances in their delete().

        Auxiliary mutator in the IConnection-ICompositeCell relation, as the parent cell of the connection.

        Args:
            connection: the internal connection to remove.
        Raises:
            AlreadyDeployedException: if this cell is already deployed
            InvalidInputException
        """
        raise NotImplementedError

    def can_have_as_internal_connection(self, connection: "IConnection") -> "BoolExplained":
        """
        Args:
            connection: connection to validate
        Returns:
            TrueExplained if this connection is a valid internal connection, FalseExplained otherwise.
        """
        raise NotImplementedError

    def get_internal_connections(self) -> "Sequence[IConnection]":
        """
        Returns:
            The internal connections of this composite cell (non-recursively).
        """
        raise NotImplementedError

    def has_as_internal_connection(self, connection: "IConnection") -> bool:
        """
        Args:
            connection: a connection object.
        Returns:
            True if this composite cell has the given connection as an internal connection, False otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_internal_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the internal connections are invalid.
        """
        raise NotImplementedError

    # ------ Topology ------

    def get_internal_topology(self) -> "Topology":
        """
        Returns:
            The internal topology of this composite cell: which of the internal cells are sources and/or sinks,
            and which internal connections are recurrent or not?
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
        """
        raise NotImplementedError

    def can_have_as_internal_topology(self, topology: Optional["Topology"]) -> "BoolExplained":
        """
        Args:
            topology: the topology to validate.
        Returns:
            TrueExplained if the given topology is a valid topology for this composite cell.
        """
        raise NotImplementedError

    def _set_internal_topology(self) -> None:
        """
        Sets the internal topology of this cell. See cell.get_internal_topology.

        Raises:
            AlreadyDeployedException: if called when the cell is already deployed.
            IndeterminableTopologyException: if the internal topology could not be determined.
        """
        raise NotImplementedError

    def _clear_internal_topology(self) -> None:
        """
        Clears the internal topology of this cell. See cell.get_internal_topology.

        Raises:
            AlreadyDeployedException: if called when the cell is already deployed.
        """
        raise NotImplementedError

    def _internal_topology_is_set(self) -> bool:
        """
        Returns:
            True if the internal topology is set, False otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_internal_topology(self) -> None:
        """
        Raises:
            InvalidStateException: if the internal topology is not valid.
        """
        raise NotImplementedError

    def _has_as_internal_recurrent_connection(self, internal_connection: "IConnection") -> bool:
        """
        Should only be used by IConnection objects: see connection.is_recurrent()

        Args:
            internal_connection: a connection object.
        Returns:
            True if the given internal connection is recurrent in the current topology, False otherwise.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
            AssertionError: if the connection is not an internal connection of this composite cell.
        """
        raise NotImplementedError

    def _has_as_internal_source_cell(self, internal_cell: "ICell") -> bool:
        """
        Should only be used by ICell objects: see cell.is_source_cell()

        Args:
            internal_cell: a cell object.
        Returns:
            True if the given internal cell is a source cell in the current topology, False otherwise.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
            AssertionError: if the cell is not an internal cell of this composite cell.
        """
        raise NotImplementedError

    def _has_as_internal_sink_cell(self, internal_cell: "ICell") -> bool:
        """
        Should only be used by ICell objects: see cell.is_sink_cell()

        Args:
            internal_cell: a cell object.
        Returns:
            True if the given internal cell is a sink cell in the current topology, False otherwise.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
            AssertionError: if the cell is not an internal cell of this composite cell.
        """
        raise NotImplementedError

    def get_internal_cells_in_topological_order(self) -> "Sequence[ICell]":
        """
        The topological order represents an order in which the cells can be executed.

        If cell_a has an outgoing connection to an input of cell_b, cell_a will occur before cell_b in the
        topological order.

        Returns:
            The internal cells of this composite cell in topological order.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
        """
        raise NotImplementedError
