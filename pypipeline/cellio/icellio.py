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
        raise NotImplementedError

    def get_name(self) -> str:
        raise NotImplementedError

    def get_full_name(self) -> str:
        raise NotImplementedError

    @classmethod
    def can_have_as_name(cls, name: str) -> BoolExplained:
        raise NotImplementedError

    def assert_has_proper_name(self) -> None:
        raise NotImplementedError

    def get_cell(self) -> "ICell":
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
        raise NotImplementedError

    def can_have_as_validation_fn(self, validation_fn: Optional[Callable[[T], bool]]) -> BoolExplained:
        raise NotImplementedError

    def assert_has_proper_validation_fn(self) -> None:
        raise NotImplementedError

    def pull(self) -> T:
        raise NotImplementedError

    def reset(self) -> None:
        raise NotImplementedError

    def get_value(self) -> T:
        raise NotImplementedError

    def _set_value(self, value: T) -> None:
        raise NotImplementedError

    def value_is_set(self) -> bool:
        raise NotImplementedError

    def can_have_as_value(self, value: T) -> bool:
        raise NotImplementedError

    def _wait_for_value(self, timeout=None) -> None:
        raise NotImplementedError

    def _acknowledge_value(self) -> None:
        raise NotImplementedError

    def assert_has_proper_value(self) -> None:
        raise NotImplementedError

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        raise NotImplementedError

    def get_nb_incoming_connections(self) -> int:
        raise NotImplementedError

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        raise NotImplementedError

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        raise NotImplementedError

    def get_nb_outgoing_connections(self) -> int:
        raise NotImplementedError

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        raise NotImplementedError

    def _deploy(self) -> None:
        raise NotImplementedError

    def _undeploy(self) -> None:
        raise NotImplementedError

    def _is_deployed(self) -> bool:
        raise NotImplementedError

    def _assert_is_properly_deployed(self) -> None:
        raise NotImplementedError

    def _get_sync_state(self) -> Dict[str, Any]:
        raise NotImplementedError

    def _set_sync_state(self, state: Dict) -> None:
        raise NotImplementedError

    def get_nb_available_pulls(self) -> Optional[int]:
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
        raise NotImplementedError

    def assert_is_valid(self) -> None:
        raise NotImplementedError

    def delete(self) -> None:
        """
        Deletes this IO, and all its internals.

        Main mutator in the IO-ICell relation, as owner of the IO.

        Raises:
            CannotBeDeletedException
        """
        raise NotImplementedError


class IInput(IO[T], Generic[T]):

    def is_provided(self) -> bool:
        """Is this input provided by a default value / incoming connection or another way of value provision. """
        raise NotImplementedError


class IOutput(IO[T], Generic[T]):

    def set_value(self, value: T) -> None:
        raise NotImplementedError

    def all_outgoing_connections_have_pulled(self) -> bool:
        raise NotImplementedError


class IConnectionEntryPoint(IO[T], Generic[T]):
    """
    Connection entrypoint interface.

    A connection entrypoint is a type of IO that accepts incoming connections.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    """

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The incoming connections of this IO.
        """
        raise NotImplementedError

    def can_have_as_incoming_connection(self, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: the connection to validate.
        Returns:
            TrueExplained if the given connection is a valid incoming connection for this IO. FalseExplained otherwise.
        """
        raise NotImplementedError

    def can_have_as_nb_incoming_connections(self, number_of_incoming_connections: int) -> BoolExplained:
        """
        Args:
            number_of_incoming_connections: the number of incoming connections to validate.
        Returns:
            TrueExplained if the given numer is a valid number of incoming connection for this IO.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def _add_incoming_connection(self, connection: "IConnection[T]") -> None:
        """
        Should only be used by IConnection objects in their __init__.

        Auxiliary mutator in the IConnection-IConnectionEntryPoint relation, as the target of the connection.

        Args:
            connection: the incoming connection to add.
        Raises:
            AlreadyDeployedException: if this IO is already deployed.
            InvalidInputException
        """
        raise NotImplementedError

    def _remove_incoming_connection(self, connection: "IConnection[T]") -> None:
        """
        Should only be used by IConnection objects in their delete().

        Auxiliary mutator in the IConnection-IConnectionEntryPoint relation, as the target of the connection.

        Args:
            connection: the incoming connection to remove.
        Raises:
            AlreadyDeployedException: if this IO is already deployed.
            InvalidInputException
        """
        raise NotImplementedError

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        raise NotImplementedError

    def get_max_nb_incoming_connections(self) -> int:
        raise NotImplementedError

    def get_nb_incoming_connections(self) -> int:
        raise NotImplementedError

    def has_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> bool:
        raise NotImplementedError

    def get_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> "IConnection[T]":
        raise NotImplementedError

    def assert_has_proper_incoming_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the incoming connections is invalid.
        """
        raise NotImplementedError


class IConnectionExitPoint(IO[T], Generic[T]):
    """
    Connection exit point interface.

    A connection exit point is a type of IO that accepts outgoing connections.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionExitPoint is the controlled class in the IConnection-IConnectionExitPoint relation, as the
    source of the connection.
    """

    def pull_as_connection(self, connection: "IConnection[T]") -> T:
        raise NotImplementedError

    def all_outgoing_connections_have_pulled(self) -> bool:
        raise NotImplementedError

    def has_seen_value(self, connection: "IConnection[T]") -> bool:
        raise NotImplementedError

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The outgoing connections of this connection exit point.
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid outgoing connection for this connection exit point.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def can_have_as_nb_outgoing_connections(self, number_of_outgoing_connections: int) -> BoolExplained:
        """
        Args:
            number_of_outgoing_connections: the number of outgoing connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of outgoing connection for this IO.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def _add_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when registering itself as outgoing connection.

        Args:
            connection: the connection to add as outgoing connection.
        Raises:
            InvalidInputException
        """
        raise NotImplementedError

    def _remove_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when unregistering itself as outgoing connection.

        Args:
            connection: the connection to remove as outgoing connection.
        Raises:
            InvalidInputException
        """
        raise NotImplementedError

    def get_max_nb_outgoing_connections(self) -> int:
        raise NotImplementedError

    def get_nb_outgoing_connections(self) -> int:
        raise NotImplementedError

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        raise NotImplementedError

    def has_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> bool:
        raise NotImplementedError

    def get_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> "IConnection[T]":
        raise NotImplementedError

    def assert_has_proper_outgoing_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the outgoing connections is invalid.
        """
        raise NotImplementedError

    def has_initial_value(self) -> bool:
        raise NotImplementedError
