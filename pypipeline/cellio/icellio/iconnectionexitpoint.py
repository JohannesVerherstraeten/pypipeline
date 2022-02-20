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
    from pypipeline.cellio.icellio.iconnectionentrypoint import IConnectionEntryPoint


T = TypeVar('T')


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
        """
        Should only be used by IConnection instances.

        Args:
            connection: an outgoing connection of this IO.
        Returns:
            The new value of this IO, which the connection hasn't seen yet.
        """
        raise NotImplementedError

    def all_outgoing_connections_have_pulled(self) -> bool:
        """
        Returns:
            True if all outgoing connections have pulled this IO (have seen its current value), False, otherwise.
        """
        raise NotImplementedError

    def has_seen_value(self, connection: "IConnection[T]") -> bool:
        """
        Args:
            connection: an outgoing connection of this IO.
        Returns:
            True if the given connection has seen the current value of this IO, False otherwise.
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> "BoolExplained":
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid outgoing connection for this connection exit point.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def can_have_as_nb_outgoing_connections(self, number_of_outgoing_connections: int) -> "BoolExplained":
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
        """
        Returns:
            The max number of outgoing connections this IO is allowed to have.
        """
        raise NotImplementedError

    def has_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> bool:
        """
        Args:
            target: a connection entry point.
        Returns:
            True if this connection exit point has an outgoing connection to the given target.
        """
        raise NotImplementedError

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

    def assert_has_proper_outgoing_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the outgoing connections is invalid.
        """
        raise NotImplementedError

    def has_initial_value(self) -> bool:
        """
        Returns:
            True if this IO has an initial value, meaning it can be used to create recurrent connections from.
        """
        raise NotImplementedError
