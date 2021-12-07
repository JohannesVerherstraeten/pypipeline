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

from typing import TypeVar, Generic, Optional, TYPE_CHECKING, Sequence, Callable

from pypipeline.cellio.ainput import AInput
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.cellio.connectionendpoint import ConnectionEntryPoint
from pypipeline.exceptions import UnconnectedException
from pypipeline.validation import BoolExplained

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio.connectionendpoint import ConnectionExitPoint

T = TypeVar('T')


class Input(AInput[T], IConnectionEntryPoint[T], Generic[T]):
    """
    Input class.

    An Input is a type of input that accepts 1 incoming connection and no outgoing connections.
    Every time an Input is pulled, it will pull the incoming connection. If no incoming connection is present,
    an UnconnectedException is raised.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    """

    def __init__(self, cell: "ICell", name: str, validation_fn: Optional[Callable[[T], bool]] = None):
        super(Input, self).__init__(cell, name, validation_fn)
        self.__entry_point: ConnectionEntryPoint[T] = ConnectionEntryPoint(self, max_incoming_connections=1)
        self._notify_observers_of_creation()

    def pull(self) -> T:
        if self.get_nb_incoming_connections() == 0:
            raise UnconnectedException(f"CellInput {self} has no incoming connection to pull")
        connection = self.get_incoming_connections()[0]
        result: T = connection.pull()
        self._set_value(result)
        return result

    def is_provided(self) -> bool:
        return self.get_nb_incoming_connections() > 0

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        return ()

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        return False

    def get_nb_outgoing_connections(self) -> int:
        return 0

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The incoming connections of this Input.
        """
        return self.__entry_point.get_incoming_connections()

    def can_have_as_incoming_connection(self, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid incoming connection for this Input.
            FalseExplained otherwise.
        """
        return self.__entry_point.can_have_as_incoming_connection(connection)

    def can_have_as_nb_incoming_connections(self, number_of_incoming_connections: int) -> BoolExplained:
        """
        Args:
            number_of_incoming_connections: the number of incoming connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of incoming connection for this Input.
            FalseExplained otherwise.
        """
        return self.__entry_point.can_have_as_nb_incoming_connections(number_of_incoming_connections)

    def _add_incoming_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionEntryPoint relation, as the target of the connection.
        -> Should only be used by an IConnection, when registering itself as incoming connection.

        Args:
            connection: the connection to add as incoming connection.
        Raises:
            InvalidInputException
        """
        self.__entry_point._add_incoming_connection(connection)     # access to protected method on purpose

    def _remove_incoming_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionEntryPoint relation, as the target of the connection.
        -> Should only be used by an IConnection, when unregistering itself as incoming connection.

        Args:
            connection: the connection to remove as incoming connection.
        Raises:
            InvalidInputException
        """
        self.__entry_point._remove_incoming_connection(connection)  # access to protected method on purpose

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        return self.__entry_point.has_as_incoming_connection(connection)

    def get_max_nb_incoming_connections(self) -> int:
        return self.__entry_point.get_max_nb_incoming_connections()

    def get_nb_incoming_connections(self) -> int:
        return self.__entry_point.get_nb_incoming_connections()

    def has_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> bool:
        return self.__entry_point.has_incoming_connection_with(source)

    def get_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> "IConnection[T]":
        return self.__entry_point.get_incoming_connection_with(source)

    def assert_has_proper_incoming_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the incoming connections is invalid.
        """
        self.__entry_point.assert_has_proper_incoming_connections()

    def _get_connection_entry_point(self) -> ConnectionEntryPoint:
        return self.__entry_point

    def _get_connection_exit_point(self) -> Optional["ConnectionExitPoint"]:
        return None

    def get_nb_available_pulls(self) -> Optional[int]:
        incoming_connections = self.get_incoming_connections()
        if len(incoming_connections) == 0:
            raise UnconnectedException(f"{str(self)} has no incoming connections. ")
        return incoming_connections[0].get_nb_available_pulls()

    def assert_is_valid(self) -> None:
        super(Input, self).assert_is_valid()
        self.__entry_point.assert_is_valid()

    def delete(self) -> None:
        """
        Deletes this IO, and all its internals.

        Main mutator in the IO-ICell relation, as owner of the IO.

        Raises:
            CannotBeDeletedException
        """
        super(Input, self).delete()
        self.__entry_point.delete()
