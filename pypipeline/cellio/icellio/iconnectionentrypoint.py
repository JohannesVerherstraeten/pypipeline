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
