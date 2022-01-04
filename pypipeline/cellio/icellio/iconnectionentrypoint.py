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

from pypipeline.cellio.icellio.io import IO

if TYPE_CHECKING:
    from pypipeline.validation import BoolExplained
    from pypipeline.connection import IConnection
    from pypipeline.cellio.icellio.iconnectionexitpoint import IConnectionExitPoint


T = TypeVar('T')


class IConnectionEntryPoint(IO[T], Generic[T]):
    """
    Connection entrypoint interface.

    A connection entrypoint is a type of IO that accepts incoming connections.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    """

    def can_have_as_incoming_connection(self, connection: "IConnection[T]") -> "BoolExplained":
        """
        Args:
            connection: the connection to validate.
        Returns:
            TrueExplained if the given connection is a valid incoming connection for this IO. FalseExplained otherwise.
        """
        raise NotImplementedError

    def can_have_as_nb_incoming_connections(self, number_of_incoming_connections: int) -> "BoolExplained":
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

    def get_max_nb_incoming_connections(self) -> int:
        """
        Returns:
            The max number of incoming connections this entrypoint is allowed to have.
        """
        raise NotImplementedError

    def has_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> bool:
        """
        Args:
            source: a connection exit point.
        Returns:
            True if this connection entry point has an incoming connection originating from the given source.
        """
        raise NotImplementedError

    def get_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> "IConnection[T]":
        """
        Args:
            source: a connection exit point.
        Returns:
            The connection originating from the given source, arriving at this connection entry point.
        Raises:
            ValueError: if no incoming connection exists from the given source to this IO.
        """
        raise NotImplementedError

    def assert_has_proper_incoming_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the incoming connections is invalid.
        """
        raise NotImplementedError
