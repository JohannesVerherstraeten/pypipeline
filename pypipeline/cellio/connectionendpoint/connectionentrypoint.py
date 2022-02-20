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
        """
        Args:
            io: the IO on which to create this connection entry point.
            max_incoming_connections: the max number of incoming connections that is allowed.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__io = io
        self.__max_incoming_connections = max_incoming_connections
        self.__incoming_connections: "List[IConnection[T]]" = []

    def get_io(self) -> "IConnectionEntryPoint[T]":
        """
        Returns:
            The IO to which this connection entry point belongs.
        """
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
        """
        if not self.has_as_incoming_connection(connection):
            raise InvalidInputException(f"{self} doesn't have {connection} as incoming connection.")
        self.__incoming_connections.remove(connection)

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        """
        Args:
            connection: a connection
        Returns:
            True if this connection entry point has the given connection as incoming connection.
        """
        return connection in self.__incoming_connections

    def get_max_nb_incoming_connections(self) -> int:
        """
        Returns:
            The max number of incoming connections this entrypoint is allowed to have.
        """
        return self.__max_incoming_connections

    def get_nb_incoming_connections(self) -> int:
        """
        Returns:
            The number of incoming connections of this connection entry point.
        """
        return len(self.__incoming_connections)

    def has_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> bool:
        """
        Args:
            source: a connection exit point.
        Returns:
            True if this connection entry point has an incoming connection originating from the given source.
        """
        for connection in self.get_incoming_connections():
            if connection.get_source() == source:
                return True
        return False

    def get_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> "IConnection[T]":
        """
        Args:
            source: a connection exit point.
        Returns:
            The connection originating from the given source, arriving at this connection entry point.
        Raises:
            ValueError: if no incoming connection exists from the given source to this connection entry point.
        """
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
