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

from typing import TypeVar, Generic, Sequence, List, TYPE_CHECKING
import logging

import pypipeline
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.exceptions import InvalidStateException, InvalidInputException
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not


if TYPE_CHECKING:
    from pypipeline.connection import IConnection

T = TypeVar('T')


class ConnectionEntryPoint(Generic[T]):
    """
    Connection entrypoint implementation. To be used inside an IO that inherits the IConnectionEntryPointIO interface.

    An ConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    """

    def __init__(self, io: "IConnectionEntryPoint[T]", max_incoming_connections: int = 1):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__io = io
        self.__max_incoming_connections = max_incoming_connections
        self.__incoming_connections: "List[IConnection[T]]" = []

    def get_io(self) -> "IConnectionEntryPoint[T]":
        return self.__io

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The incoming connections of this connection entry point.
        """
        return tuple(self.__incoming_connections)

    def can_have_as_incoming_connection(self, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid incoming connection for this connection entry point.
            FalseExplained otherwise.
        """
        if not isinstance(connection, pypipeline.connection.IConnection):
            return FalseExplained(f"Incoming connection must be instance of IConnection, got{connection}")
        return TrueExplained()

    def can_have_as_nb_incoming_connections(self, number_of_incoming_connections: int) -> BoolExplained:
        """
        Args:
            number_of_incoming_connections: the number of incoming connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of incoming connection for this IO.
            FalseExplained otherwise.
        """
        max_nb = self.get_max_nb_incoming_connections()
        if not 0 <= number_of_incoming_connections <= max_nb:
            return FalseExplained(f"{self.get_io()}: the number of incoming connections should be in range "
                                  f"0..{max_nb} (inclusive), got {number_of_incoming_connections}.")
        return TrueExplained()

    def _add_incoming_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionEntryPoint relation, as the target of the connection.
        -> Should only be used by an IConnection, when registering itself as incoming connection.

        Args:
            connection: the connection to add as incoming connection.
        Raises:
            InvalidInputException
        """
        raise_if_not(self.can_have_as_incoming_connection(connection), InvalidInputException)
        raise_if_not(self.can_have_as_nb_incoming_connections(self.get_nb_incoming_connections()+1),
                     InvalidInputException)
        if self.has_as_incoming_connection(connection):
            raise InvalidInputException(f"{self} already has {connection} as incoming connection.")
        self.__incoming_connections.append(connection)

    def _remove_incoming_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionEntryPoint relation, as the target of the connection.
        -> Should only be used by an IConnection, when unregistering itself as incoming connection.

        Args:
            connection: the connection to remove as incoming connection.
        Raises:
            InvalidInputException
        """
        if not self.has_as_incoming_connection(connection):
            raise InvalidInputException(f"{self} doesn't have {connection} as incoming connection.")
        self.__incoming_connections.remove(connection)

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        return connection in self.__incoming_connections

    def get_max_nb_incoming_connections(self) -> int:
        return self.__max_incoming_connections

    def get_nb_incoming_connections(self) -> int:
        return len(self.__incoming_connections)

    def has_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> bool:
        for connection in self.get_incoming_connections():
            if connection.get_source() == source:
                return True
        return False

    def get_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> "IConnection[T]":
        for connection in self.get_incoming_connections():
            if connection.get_source() == source:
                return connection
        raise ValueError(f"No connection exists between {source} and {self}")

    def assert_has_proper_incoming_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the incoming connections is invalid.
        """
        raise_if_not(self.can_have_as_nb_incoming_connections(self.get_nb_incoming_connections()),
                     InvalidStateException)

        for connection in self.get_incoming_connections():
            raise_if_not(self.can_have_as_incoming_connection(connection), InvalidStateException,
                         f"{self.get_io()} has invalid incoming connection: ")
            if connection.get_target() != self.get_io():
                raise InvalidStateException(f"Inconsistent relation: {connection} doesn't have {self.get_io()} as "
                                            f"target.")

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the incoming connections is invalid.
        """
        self.assert_has_proper_incoming_connections()

    def delete(self) -> None:
        """
        Should only be used by the owning IConnectionEntryPointIO.
        """
        assert self.get_nb_incoming_connections() == 0
        self.__incoming_connections = None      # type: ignore
        self.__io = None                        # type: ignore