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

from typing import TypeVar, Generic, TYPE_CHECKING
from abc import ABC, abstractmethod

from pypipeline.cellio.icellio.io import IO

if TYPE_CHECKING:
    from pypipeline.validation import BoolExplained
    from pypipeline.connection import IConnection
    from pypipeline.cellio.icellio.iconnectionentrypoint import IConnectionEntryPoint


T = TypeVar('T')


class IConnectionExitPoint(IO[T], ABC, Generic[T]):
    """
    Connection exit point interface.

    A connection exit point is a type of IO that accepts outgoing connections.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionExitPoint is the controlled class in the IConnection-IConnectionExitPoint relation, as the
    source of the connection.
    """

    @abstractmethod
    def pull_as_connection(self, connection: "IConnection[T]") -> T:
        """
        Should only be used by IConnection instances.

        Args:
            connection: an outgoing connection of this IO.
        Returns:
            The new value of this IO, which the connection hasn't seen yet.
        """
        raise NotImplementedError

    @abstractmethod
    def all_outgoing_connections_have_pulled(self) -> bool:
        """
        Returns:
            True if all outgoing connections have pulled this IO (have seen its current value), False, otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def has_seen_value(self, connection: "IConnection[T]") -> bool:
        """
        Args:
            connection: an outgoing connection of this IO.
        Returns:
            True if the given connection has seen the current value of this IO, False otherwise.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> "BoolExplained":
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid outgoing connection for this connection exit point.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def can_have_as_nb_outgoing_connections(self, number_of_outgoing_connections: int) -> "BoolExplained":
        """
        Args:
            number_of_outgoing_connections: the number of outgoing connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of outgoing connection for this IO.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    def get_max_nb_outgoing_connections(self) -> int:
        """
        Returns:
            The max number of outgoing connections this IO is allowed to have.
        """
        raise NotImplementedError

    @abstractmethod
    def has_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> bool:
        """
        Args:
            target: a connection entry point.
        Returns:
            True if this connection exit point has an outgoing connection to the given target.
        """
        raise NotImplementedError

    @abstractmethod
    def get_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> "IConnection[T]":
        """
        Args:
            target: a connection entry point.
        Returns:
            The connection which starts at this IO and goes to the given target.
        Raises:
            ValueError: if no outgoing connection exists from this IO to the given target.
        """
        raise NotImplementedError

    @abstractmethod
    def assert_has_proper_outgoing_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the outgoing connections is invalid.
        """
        raise NotImplementedError

    @abstractmethod
    def has_initial_value(self) -> bool:
        """
        Returns:
            True if this IO has an initial value, meaning it can be used to create recurrent connections from.
        """
        raise NotImplementedError
