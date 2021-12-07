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

from typing import TypeVar, Generic, TYPE_CHECKING, Sequence, Optional

import pypipeline
from pypipeline.cellio.ainput import AInput
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.cellio.connectionendpoint import ConnectionExitPoint, ConnectionEntryPoint
from pypipeline.exceptions import UnconnectedException
from pypipeline.validation import BoolExplained, FalseExplained, TrueExplained

if TYPE_CHECKING:
    from pypipeline.cell import ICell, ICompositeCell
    from pypipeline.connection import IConnection


T = TypeVar('T')


class InputPort(AInput[T], IConnectionEntryPoint[T], IConnectionExitPoint[T], Generic[T]):
    """
    InputPort class.

    An input port is a type of input that can only be created for a composite cell.
    It accepts 1 incoming connection and infinite outgoing (internal) connections.
    Every time an input port is pulled, it will pull its incoming connection. If no incoming connection is present,
    an UnconnectedException is raised.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    An IConnectionExitPoint is the controlled class in the Ionnection-IConnectionExitPoint relation, as the
    source of the connection.

    TODO assert no recurrent connection can enter or leave an InputPort.
    """

    def __init__(self, cell: "ICompositeCell", name: str):
        super(InputPort, self).__init__(cell, name)
        self.__entry_point: ConnectionEntryPoint[T] = ConnectionEntryPoint(self, max_incoming_connections=1)
        self.__exit_point: ConnectionExitPoint[T] = ConnectionExitPoint(self, max_outgoing_connections=99999)
        self._notify_observers_of_creation()

    def can_have_as_cell(self, cell: "ICell") -> BoolExplained:
        super_result = super(InputPort, self).can_have_as_cell(cell)
        if not super_result:
            return super_result
        if not isinstance(cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell):
            return FalseExplained(f"An InputPort can only be created on an instance of ICompositeCell")
        return TrueExplained()

    def pull(self) -> T:
        self.logger.debug(f"{self}.pull()")
        if self.get_nb_incoming_connections() == 0:
            raise UnconnectedException(f"CellInput {self} has no incoming connection to pull")
        connection = self.get_incoming_connections()[0]
        self.logger.debug(f"{self} pulling connection {connection}")
        result: T = connection.pull()
        self._set_value(result)
        return result

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The incoming connections of this input port.
        """
        return self.__entry_point.get_incoming_connections()

    def can_have_as_incoming_connection(self, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid incoming connection for this input port.
            FalseExplained otherwise.
        """
        return self.__entry_point.can_have_as_incoming_connection(connection)

    def can_have_as_nb_incoming_connections(self, number_of_incoming_connections: int) -> BoolExplained:
        """
        Args:
            number_of_incoming_connections: the number of incoming connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of incoming connection for this input port.
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
        self.__entry_point._add_incoming_connection(connection)

    def _remove_incoming_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionEntryPoint relation, as the target of the connection.
        -> Should only be used by an IConnection, when unregistering itself as incoming connection.

        Args:
            connection: the connection to remove as incoming connection.
        Raises:
            InvalidInputException
        """
        self.__entry_point._remove_incoming_connection(connection)

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

    def pull_as_connection(self, connection: "IConnection[T]") -> T:
        return self.__exit_point.pull_as_connection(connection)

    def all_outgoing_connections_have_pulled(self) -> bool:
        return self.__exit_point.have_all_outgoing_connections_pulled()

    def has_seen_value(self, connection: "IConnection[T]") -> bool:
        return self.__exit_point.has_seen_value(connection)

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The outgoing connections of this input port.
        """
        return self.__exit_point.get_outgoing_connections()

    @classmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid outgoing connection for this input port.
            FalseExplained otherwise.
        """
        return ConnectionExitPoint.can_have_as_outgoing_connection(connection)

    def can_have_as_nb_outgoing_connections(self, number_of_outgoing_connections: int) -> BoolExplained:
        """
        Args:
            number_of_outgoing_connections: the number of outgoing connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of outgoing connection for this input port.
            FalseExplained otherwise.
        """
        return self.__exit_point.can_have_as_nb_outgoing_connections(number_of_outgoing_connections)

    def _add_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when registering itself as outgoing connection.

        Args:
            connection: the connection to add as outgoing connection.
        Raises:
            InvalidInputException
        """
        self.__exit_point._add_outgoing_connection(connection)

    def _remove_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when unregistering itself as outgoing connection.

        Args:
            connection: the connection to remove as outgoing connection.
        Raises:
            InvalidInputException
        """
        self.__exit_point._remove_outgoing_connection(connection)

    def get_max_nb_outgoing_connections(self) -> int:
        return self.__exit_point.get_max_nb_outgoing_connections()

    def get_nb_outgoing_connections(self) -> int:
        return self.__exit_point.get_nb_outgoing_connections()

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        return self.__exit_point.has_as_outgoing_connection(connection)

    def has_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> bool:
        return self.__exit_point.has_outgoing_connection_to(target)

    def get_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> "IConnection[T]":
        return self.__exit_point.get_outgoing_connection_to(target)

    def assert_has_proper_outgoing_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the outgoing connections is invalid.
        """
        self.__exit_point.assert_has_proper_outgoing_connections()

    def has_initial_value(self) -> bool:
        return self.__exit_point.has_initial_value()

    def is_provided(self) -> bool:
        return self.get_nb_incoming_connections() > 0

    def _get_connection_entry_point(self) -> ConnectionEntryPoint:
        return self.__entry_point

    def _get_connection_exit_point(self) -> ConnectionExitPoint:
        return self.__exit_point

    def get_nb_available_pulls(self) -> Optional[int]:
        with self._get_state_lock():
            incoming_connections = self.get_incoming_connections()
            if len(incoming_connections) == 0:
                raise UnconnectedException(f"{str(self)} has no incoming connections. ")
            return incoming_connections[0].get_nb_available_pulls()

    def assert_is_valid(self) -> None:
        super(InputPort, self).assert_is_valid()
        self.__entry_point.assert_is_valid()
        self.__exit_point.assert_is_valid()

    def delete(self) -> None:
        """
        Deletes this IO, and all its internals.

        Main mutator in the IO-ICell relation, as owner of the IO.

        Raises:
            CannotBeDeletedException
        """
        super(InputPort, self).delete()
        self.__entry_point.delete()
        self.__exit_point.delete()
