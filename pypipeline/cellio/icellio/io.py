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

from typing import TypeVar, Generic, Optional, Dict, TYPE_CHECKING, Any, Callable, Sequence

from pypipeline.validation import BoolExplained

if TYPE_CHECKING:
    from threading import RLock

    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection


T = TypeVar('T')


class IO(Generic[T]):
    """
    Cell IO interface.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    """

    def _get_cell_pull_lock(self) -> "RLock":
        """
        Returns:
            The pull lock which makes sure that the cell of this IO is not pulled concurrently.
        """
        raise NotImplementedError

    def get_name(self) -> str:
        """
        Returns:
            The name of this IO.
        """
        raise NotImplementedError

    def get_full_name(self) -> str:
        """
        Returns:
            The name of this IO, preceded by the name of its cell and all its parent cells. Ex: If the name of this
            IO is "MyCellIO", the full name might be "ParentCellName.CellName.MyCellIO".
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_name(cls, name: str) -> BoolExplained:
        """
        Args:
            name: the name to validate.
        Returns:
            TrueExplained if the given name is a valid name for this IO, FalseExplained otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_name(self) -> None:
        """
        Raises:
            InvalidStateException: if the name of this IO is not a valid name.
        """
        raise NotImplementedError

    def get_cell(self) -> "ICell":
        """
        Returns:
            The cell to which this IO belongs.
        """
        raise NotImplementedError

    def can_have_as_cell(self, cell: "ICell") -> BoolExplained:
        """
        Args:
            cell: cell object to validate.
        Returns:
            TrueExplained if the given cell is a valid cell for this IO. FalseExplained otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_cell(self) -> None:
        """
        Raises:
            InvalidStateException: if the cell of this IO is not valid.
        """
        raise NotImplementedError

    def get_validation_fn(self) -> Optional[Callable[[T], bool]]:
        """
        Returns:
            The validation function configured for the value of this IO. If configured, every new value that is set
            on this IO will be validated with this function first.
        """
        raise NotImplementedError

    def can_have_as_validation_fn(self, validation_fn: Optional[Callable[[T], bool]]) -> BoolExplained:
        """
        Args:
            validation_fn: the validation function to validate.
        Returns:
            TrueExplained if the given validation function is a valid validation function for this IO.
        """
        raise NotImplementedError

    def assert_has_proper_validation_fn(self) -> None:
        """
        Raises:
            InvalidStateException: if the validation function of this IO is not valid.
        """
        raise NotImplementedError

    def pull(self) -> T:
        """
        Pulling an IO requests a new value to be set. It will block until the new value is available.

        Depending on the type of IO, the value is requested in different ways. Ex: inputs may pull a new value from
        incoming connections, while outputs may pull their cell to request a new value.

        Returns:
            The new value that is pulled.
        """
        raise NotImplementedError

    def reset(self) -> None:
        """
        Resets the state of the IO (ex: its current value).

        Just like the pull() method, this reset is recursively propagated through all prerequisite cells
        up to the source cells.
        """
        raise NotImplementedError

    def get_value(self) -> T:
        """
        Returns:
            The value that is currently set for this IO.
        Raises:
            NoInputProvidedException: if no value has been set.
        """
        raise NotImplementedError

    def _set_value(self, value: T) -> None:
        """
        Args:
            value: the new value for this IO.
        Raises:
            InvalidInputException: the given value is invalid.
        """
        raise NotImplementedError

    def value_is_set(self) -> bool:
        """
        Returns:
            True if a value is set for this IO.
        """
        raise NotImplementedError

    def can_have_as_value(self, value: T) -> bool:
        """
        Args:
            value: the value to validate.
        Returns:
            True if the given value is a valid value for this IO.
        """
        raise NotImplementedError

    def _wait_for_value(self, interruption_frequency: Optional[float] = None) -> None:
        """
        Blocking call that only returns when a new value is available.

        Args:
            interruption_frequency: the frequency in seconds, determining how often the call will check whether
                this IO is still deployed. If the IO is still deployed during the interruptions, this method call keeps
                blocking and waiting for a new value. Otherwise, it will raise a NotDeployedException.
                If None, the method never checks the deployment status of the IO (not recommended).
        """
        raise NotImplementedError

    def _acknowledge_value(self) -> None:
        """
        TODO should this method be part of the general interface?
        """
        raise NotImplementedError

    def assert_has_proper_value(self) -> None:
        """
        Raises:
            InvalidStateException: if the value of IO cell is invalid.
        """
        raise NotImplementedError

    def get_all_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            All connections (both incoming and outgoing) of this IO.
        """
        raise NotImplementedError

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            All incoming connections of this IO.
        """
        raise NotImplementedError

    def get_nb_incoming_connections(self) -> int:
        """
        Returns:
            The number of incoming connections of this IO.
        """
        raise NotImplementedError

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        """
        Args:
            connection: a connection object.
        Returns:
            True if the given connection is an incoming connection of this IO.
        """
        raise NotImplementedError

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            All outgoing connections of this IO.
        """
        raise NotImplementedError

    def get_nb_outgoing_connections(self) -> int:
        """
        Returns:
            The number of outgoing connections of this IO.
        """
        raise NotImplementedError

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        """
        Returns:
            True if the given connection is an outgoing connection of this IO.
        """
        raise NotImplementedError

    def _deploy(self) -> None:
        """
        Deploy this IO.
        """
        raise NotImplementedError

    def _undeploy(self) -> None:
        """
        UnDeploy this IO.
        """
        raise NotImplementedError

    def _is_deployed(self) -> bool:
        """
        Returns:
            True if this IO is deployed, False otherwise.
        """
        raise NotImplementedError

    def _assert_is_properly_deployed(self) -> None:
        """
        Raises:
            InvalidStateException: if this IO is not properly deployed.
        """
        raise NotImplementedError

    def _get_sync_state(self) -> Dict[str, Any]:
        """
        Used for synchronizing the state clone IOs with the state of their corresponding original ones.

        Returns:
            A (nested) dictionary, containing the state of this IO.
        """
        raise NotImplementedError

    def _set_sync_state(self, state: Dict) -> None:
        """
        Used for synchronizing the state of clone IOs with the state of their corresponding original ones.

        Args:
            state: a (nested) dictionary, containing the new state of this IO.
        """
        raise NotImplementedError

    def get_nb_available_pulls(self) -> Optional[int]:
        """
        Returns the total number of times this IO can be pulled.

        Returns:
            The total number of times this IO can be pulled, or
            None if the IO can be pulled infinitely or an unspecified amount of time.
        Raises:
            IndeterminableTopologyException: if the topology of the cell to which this IO belongs could not be
                determined.
        """
        raise NotImplementedError

    def _is_optional_even_when_typing_says_otherwise(self) -> bool:
        """
        Are None values allowed to be set at this IO, even when the IO generic type is not an Optional?

        Used to determine whether this IO (or all downstream IO that are connected to this IO) can deal
        with incoming None values (ex: RuntimeParameter).

        Used in the FastAPIServer to annotate whether an IO is Optional in the openapi spec.
        """
        raise NotImplementedError

    def get_topology_description(self) -> Dict[str, Any]:
        """
        Returns:
            A dictionary (json format), fully describing the topological properties if this IO.
        Raises:
            IndeterminableTopologyException: if the topology of the cell to which this IO belongs could not be
                determined.
        """
        raise NotImplementedError

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if any of the relations/attributes of this IO are invalid.
        """
        raise NotImplementedError

    def delete(self) -> None:
        """
        Deletes this IO, and all its internals.

        Main mutator in the IO-ICell relation, as owner of the IO.

        Raises:
            CannotBeDeletedException
        """
        raise NotImplementedError
