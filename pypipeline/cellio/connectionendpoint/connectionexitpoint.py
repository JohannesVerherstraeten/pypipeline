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

from typing import TypeVar, Generic, Dict, Sequence, TYPE_CHECKING
from threading import RLock, Condition
import logging

import pypipeline
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.exceptions import InvalidStateException, NotDeployedException, InvalidInputException
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not


if TYPE_CHECKING:
    from pypipeline.connection import IConnection

T = TypeVar('T')


class ConnectionExitPoint(Generic[T]):
    """
    Connection exit point implementation. To be used inside an IO that inherits the IConnectionExitPointIO interface.

    A ConnectionExitPoint is the controlled class in the IConnection-IConnectionExitPoint relation, as the
    source of the connection.
    """

    PULL_TIMEOUT: float = 5.0

    def __init__(self, io: "IConnectionExitPoint[T]", max_outgoing_connections: int = 99999):
        """
        Args:
            io: the IO on which to create this connection exit point.
            max_outgoing_connections: the max number of outgoing connections that is allowed.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__io = io
        self.__max_outgoing_connections = max_outgoing_connections
        self.__has_seen_value: "Dict[IConnection[T], bool]" = dict()
        self._pull_lock = Condition(RLock())

    def get_io(self) -> "IConnectionExitPoint[T]":
        """
        Returns:
            The IO to which this connection entry point belongs.
        """
        return self.__io

    def pull_as_connection(self, connection: "IConnection[T]") -> T:
        """
        Pull executed by a connection.

        Outputs should only pull their cell again when all their connections have pulled the previous result.
        This is to avoid that the cell is executed again while another downstream cell still needed the old output
        value.

        Note: calling this method may block forever when a connection pulls twice before all other connections have
        pulled.

        Args:
            connection: the outgoing connection that wants to pull this connection exit point.
        """
        self.logger.debug(f"{self}.pull_as_connection() @ ConnectionExitPoint")

        # This full method is a critical section
        with self._pull_lock:
            self.logger.debug(f"{self} flag1")

            assert self.has_as_outgoing_connection(connection)

            # Check whether the cell of this output ever has been pulled already.
            # If not, immediately skip to the end of this method. Otherwise:
            while self.get_io().value_is_set():
                self.logger.debug(f"{self} flag2")

                # Check whether this connection has not yet seen the result.
                if not self.has_seen_value(connection):
                    self.logger.debug(f"{self} flag3")
                    result = self.get_io().get_value()
                    self._set_has_seen_value(connection)
                    self.logger.debug(f"{self}.pull_as_connection() done")
                    return result

                # Check if any other outgoing connection still needs to see the current output value before a new one
                # is pulled. If so, wait until another connection has pulled, and then let this connection try pulling
                # again.
                elif not self.have_all_outgoing_connections_pulled():
                    self.logger.debug(f"{self} flag4")
                    # Wait before trying to pull again (continue). If waiting times out, check if this connection hasn't
                    # become undeployed.
                    while not self._pull_lock.wait(timeout=self.PULL_TIMEOUT):
                        if not connection._is_deployed():
                            raise NotDeployedException(f"Pulling connection {connection} got undeployed while busy. ")
                    continue

                # All connections, including this one, have seen the value, so it's time to pull a new one.
                else:
                    self.logger.debug(f"{self} flag5")
                    break

            self.logger.debug(f"{self} flag6")

            # Pull a new value
            with self.get_io()._get_cell_pull_lock():
                result = self.get_io().pull()
                self._set_has_seen_value(connection)

        self.logger.debug(f"{self}.pull_as_connection() done")
        return result

    def have_all_outgoing_connections_pulled(self) -> bool:
        """
        Returns:
            True if all outgoing connections have pulled this connection exit point. Used to determine whether the IO
                must wait for or request a new value.
        """
        all_have_pulled = True
        # self.logger.debug(f"{self} outgoing connections: {list(self.get_outgoing_connections())}")
        for outgoing_connection in self.get_outgoing_connections():
            # access to protected member on purpose:
            self.logger.debug(f"{outgoing_connection} has seen value: {self.has_seen_value(outgoing_connection)}")
            if outgoing_connection._is_deployed() and not self.has_seen_value(outgoing_connection):
                all_have_pulled = False
                break
        # self.logger.debug(f"{self} all outgoing connections have pulled: {all_have_pulled}")
        return all_have_pulled

    def _notify_new_value(self) -> None:
        """
        Notifies this connection exit point that a new value is available.
        """
        self._reset_has_seen_value()

    def has_seen_value(self, connection: "IConnection[T]") -> bool:
        """
        Args:
            connection: an outgoing connection of this connection exit point.
        Returns:
            True if the given connection has already seen the current value.
        """
        assert self.has_as_outgoing_connection(connection)
        return self.__has_seen_value[connection]

    def _set_has_seen_value(self, connection: "IConnection[T]") -> None:
        """
        Notifies this connection exit point that the given connection has seen the current value.

        Args:
            connection: an outgoing connection of this connection exit point.
        """
        assert self.has_as_outgoing_connection(connection)
        self.__has_seen_value[connection] = True
        if self.have_all_outgoing_connections_pulled():
            self.logger.debug(f"{self}: all outgoing connections have pulled -> ack and reset")
            self.get_io()._acknowledge_value()
        self._pull_lock.notify_all()
        # Notify the cell that an outgoing connection has successfully pulled, and that another output waiting for
        # this to happen, may be activated again.
        self.get_io().get_cell()._notify_connection_has_pulled()    # access to protected member on purpose

    def _reset_has_seen_value(self) -> None:
        """
        Clears the has_seen_value flag for every outgoing connection of this connection exit point.
        """
        for connection in self.get_outgoing_connections():
            self.__has_seen_value[connection] = False

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The outgoing connections of this connection exit point.
        """
        return tuple(self.__has_seen_value.keys())

    @classmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid outgoing connection for this connection exit point.
            FalseExplained otherwise.
        """
        if not isinstance(connection, pypipeline.connection.IConnection):
            return FalseExplained(f"Outgoing connection must be instance of IConnection, got {connection}")
        return TrueExplained()

    def can_have_as_nb_outgoing_connections(self, number_of_outgoing_connections: int) -> BoolExplained:
        """
        Args:
            number_of_outgoing_connections: the number of outgoing connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of outgoing connection for this IO.
            FalseExplained otherwise.
        """
        max_nb = self.get_max_nb_outgoing_connections()
        if not 0 <= number_of_outgoing_connections <= max_nb:
            return FalseExplained(f"{self.get_io()}: the number of outgoing connections should be in range "
                                  f"0..{max_nb} (inclusive), got {number_of_outgoing_connections}.")
        return TrueExplained()

    def _add_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when registering itself as outgoing connection.

        Args:
            connection: the connection to add as outgoing connection.
        Raises:
            InvalidInputException
        """
        raise_if_not(self.can_have_as_outgoing_connection(connection), InvalidInputException)
        raise_if_not(self.can_have_as_nb_outgoing_connections(self.get_nb_outgoing_connections()+1),
                     InvalidInputException)
        if self.has_as_outgoing_connection(connection):
            raise InvalidInputException(f"{self} already has {connection} as outgoing connection.")
        self.__has_seen_value[connection] = False

    def _remove_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when unregistering itself as outgoing connection.

        Args:
            connection: the connection to remove as outgoing connection.
        Raises:
            InvalidInputException
        """
        if not self.has_as_outgoing_connection(connection):
            raise InvalidInputException(f"{self} doesn't have {connection} as outgoing connection.")
        del self.__has_seen_value[connection]

    def get_max_nb_outgoing_connections(self) -> int:
        """
        Returns:
            The max number of outgoing connections this connection exit point is allowed to have.
        """
        return self.__max_outgoing_connections

    def get_nb_outgoing_connections(self) -> int:
        """
        Returns:
            The number of outgoing connections of this connection exit point.
        """
        return len(self.__has_seen_value)

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        """
        Returns:
            True if the given connection is an outgoing connection of this connection exit point.
        """
        return connection in self.__has_seen_value

    def has_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> bool:
        """
        Args:
            target: a connection entry point.
        Returns:
            True if this connection exit point has an outgoing connection to the given target.
        """
        for connection in self.get_outgoing_connections():
            if connection.get_target() == target:
                return True
        return False

    def get_outgoing_connection_to(self, target: "IConnectionEntryPoint[T]") -> "IConnection[T]":
        """
        Args:
            target: a connection entry point.
        Returns:
            The connection which starts at this IO and goes to the given target.
        Raises:
            ValueError: if no outgoing connection exists from this IO to the given target.
        """
        for connection in self.get_outgoing_connections():
            if connection.get_target() == target:
                return connection
        raise ValueError(f"No connection exists between {self} and {target}")

    def assert_has_proper_outgoing_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the outgoing connections is invalid.
        """
        raise_if_not(self.can_have_as_nb_outgoing_connections(self.get_nb_outgoing_connections()),
                     InvalidStateException)
        for connection in self.get_outgoing_connections():
            raise_if_not(self.can_have_as_outgoing_connection(connection), InvalidStateException,
                         f"{self} has invalid outgoing connection: ")
            if connection.get_source() != self.get_io():
                raise InvalidStateException(f"Inconsistent relation: {connection} doesn't have {self.get_io()} as "
                                            f"source")

    def has_initial_value(self) -> bool:
        """
        Returns:
            True if this connection exit point has an initial value available.
        """
        return False

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if any of the relations/attributes of this connection exit point are invalid.
        """
        self.assert_has_proper_outgoing_connections()

    def delete(self) -> None:
        """
        Should only be used by the owning IConnectionExitPointIO.
        """
        assert self.get_nb_outgoing_connections() == 0
        self.__has_seen_value = None            # type: ignore
        self.__io = None                        # type: ignore

    def __getstate__(self) -> Dict:
        # called during pickling
        # If you make changes here, these changes need to be made everywhere this method is overridden
        new_state = dict(self.__dict__)
        new_state["_pull_lock"] = None              # Locks cannot be pickled
        return new_state

    def __setstate__(self, state: Dict) -> None:
        # called during unpickling
        # If you make changes here, these changes need to be made everywhere this method is overridden
        self.__dict__ = state
        self.__dict__["_pull_lock"] = Condition(RLock())

    def __str__(self) -> str:
        return f"{self.get_io()}.{self.__class__.__name__}()"
