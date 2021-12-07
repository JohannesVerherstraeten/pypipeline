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

from typing import TypeVar, Generic, Optional, Dict, TYPE_CHECKING
from threading import RLock, Condition

from pypipeline.cellio.icellio import IConnectionExitPoint
from pypipeline.cellio.connectionendpoint.connectionexitpoint import ConnectionExitPoint
from pypipeline.exceptions import InvalidStateException, NotDeployedException

if TYPE_CHECKING:
    from pypipeline.connection import IConnection

T = TypeVar('T')


class RecurrentConnectionExitPoint(ConnectionExitPoint[T], Generic[T]):
    """
    Recurrent connection exit point implementation. In contrast to the standard ConnectionExitPoint, this
    exit point supports outgoing recurrent connections.

    To be used inside an IO that inherits the IConnectionExitPointIO interface.

    A ConnectionExitPoint is the controlled class in the IConnection-IConnectionExitPoint relation, as the
    source of the connection.
    """

    def __init__(self,
                 io: "IConnectionExitPoint[T]",
                 max_outgoing_connections: int = 99999,
                 initial_value: Optional[T] = None):
        super(RecurrentConnectionExitPoint, self).__init__(io, max_outgoing_connections=max_outgoing_connections)
        self.__has_seen_value_recurrently: "Dict[IConnection[T], bool]" = dict()
        self.__initial_value: Optional[T] = initial_value
        self._new_value_lock = Condition(RLock())

    def pull_as_connection(self, connection: "IConnection[T]") -> T:
        """
        Pull executed by a connection.

        Outputs should only pull their cell again when all their connections have pulled the previous result.
        This is to avoid that the cell is executed again while another downstream cell still needed the old output
        value.

        Note: calling this method may block forever when a connection pulls twice before all other connections have
        pulled. If a timeout is provided, a TimeoutException is raised in this case.
        """
        self.logger.debug(f"{self}.pull_as_connection() @ RecurrentConnectionExitPoint")

        assert connection in self.get_outgoing_connections()

        if connection.is_recurrent():

            # If this connection is recurrent, it must have an initial value.
            assert self.__initial_value is not None

            # If this recurrent connection has already seen its output value, wait until a new value is set
            with self._new_value_lock:
                while self.has_seen_value(connection):
                    self.logger.debug(f"flag recurrent 1")
                    if not self._new_value_lock.wait(timeout=self.PULL_TIMEOUT):
                        self.logger.info(f"recurrent connection waiting for a new value to be set...")
                        if not self.get_io()._is_deployed():
                            raise NotDeployedException(f"{self} is being pulled while not deployed")

            # If the recurrent connection hasn't seen the current value yet, return it
            self.logger.debug(f"flag recurrent 2")
            with self._pull_lock:
                self.logger.debug(f"flag recurrent 2.1")
                self.logger.debug(f"Returning initial value: {not self.get_io().value_is_set()}")
                result: T = self.get_io().get_value() if self.get_io().value_is_set() else self.__initial_value
                self._set_has_seen_value(connection)
                return result

        else:
            return super(RecurrentConnectionExitPoint, self).pull_as_connection(connection)

    def have_all_outgoing_connections_pulled(self) -> bool:
        all_have_pulled = True
        for outgoing_connection in self.get_outgoing_connections():
            if outgoing_connection._is_deployed() \
                    and not self.has_seen_value(outgoing_connection):
                all_have_pulled = False
                break
        return all_have_pulled

    def _add_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when registering itself as outgoing connection.

        Args:
            connection: the connection to add as outgoing connection.
        Raises:
            InvalidInputException
        """
        super(RecurrentConnectionExitPoint, self)._add_outgoing_connection(connection)
        self.__has_seen_value_recurrently[connection] = False

    def _remove_outgoing_connection(self, connection: "IConnection[T]") -> None:
        """
        Auxiliary mutator in the IConnection-IConnectionExitPoint relation, as the source of the connection.
        -> Should only be used by an IConnection, when unregistering itself as outgoing connection.

        Args:
            connection: the connection to remove as outgoing connection.
        Raises:
            InvalidInputException
        """
        super(RecurrentConnectionExitPoint, self)._remove_outgoing_connection(connection)
        del self.__has_seen_value_recurrently[connection]

    def _notify_new_value(self) -> None:
        super(RecurrentConnectionExitPoint, self)._notify_new_value()
        with self._new_value_lock:
            self._new_value_lock.notify_all()

    def _set_has_seen_value(self, connection: "IConnection[T]") -> None:
        # First update self before updating super, as the super class notifies waiting threads that will
        # immediately request the status of self.
        self.__has_seen_value_recurrently[connection] = connection.is_recurrent()
        super(RecurrentConnectionExitPoint, self)._set_has_seen_value(connection)

    def has_seen_value_recurrently(self, connection: "IConnection[T]") -> bool:
        assert self.has_as_outgoing_connection(connection)
        return self.__has_seen_value_recurrently[connection]

    def assert_has_proper_outgoing_connections(self) -> None:
        super(RecurrentConnectionExitPoint, self).assert_has_proper_outgoing_connections()
        if set(self.get_outgoing_connections()) != set(self.__has_seen_value_recurrently.keys()):
            raise InvalidStateException(f"Unexpectedly not the same: {set(self.get_outgoing_connections())} and "
                                        f"{set(self.__has_seen_value_recurrently.keys())}")

    def has_initial_value(self) -> bool:
        return self.__initial_value is not None
    
    def delete(self) -> None:
        """
        Should only be used by the owning IConnectionExitPointIO.
        """
        super(RecurrentConnectionExitPoint, self).delete()
        self.__has_seen_value_recurrently = None        # type: ignore

    def __getstate__(self) -> Dict:
        # called during pickling
        # If you make changes here, these changes need to be made everywhere this method is overridden
        new_state = dict(self.__dict__)
        new_state["_pull_lock"] = None              # Locks cannot be pickled
        new_state["_new_value_lock"] = None
        return new_state

    def __setstate__(self, state: Dict) -> None:
        # called during unpickling
        # If you make changes here, these changes need to be made everywhere this method is overridden
        self.__dict__ = state
        self.__dict__["_pull_lock"] = Condition(RLock())
        self.__dict__["_new_value_lock"] = Condition(RLock())
