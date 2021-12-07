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

from typing import TypeVar, Generic, TYPE_CHECKING, Optional, Dict, Sequence
from threading import BoundedSemaphore

import pypipeline
from pypipeline.cellio.ainput import AInput
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.cellio.connectionendpoint import ConnectionExitPoint, ConnectionEntryPoint
from pypipeline.validation import BoolExplained, FalseExplained, TrueExplained
from pypipeline.exceptions import NotDeployedException

if TYPE_CHECKING:
    from pypipeline.cell import ICell, ICompositeCell
    from pypipeline.connection import IConnection


T = TypeVar('T')


class InternalInput(AInput[T], IConnectionExitPoint[T], Generic[T]):
    """
    InternalInput class.

    An internal input is a type of input that can only be created on a composite cell.
    It accepts no incoming connections and infinite outgoing (internal) connections.
    Every time an internal input is pulled, it blocks and wait until a new value is set.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionExitPoint is the controlled class in the IConnection-IConnectionExitPoint relation, as the
    source of the connection.
    """

    PULL_TIMEOUT: float = 5.

    def __init__(self, cell: "ICompositeCell", name: str):
        super(InternalInput, self).__init__(cell, name)
        self.__exit_point: ConnectionExitPoint[T] = ConnectionExitPoint(self, max_outgoing_connections=99999)
        self.__value_is_acknowledged: BoundedSemaphore = BoundedSemaphore(1)
        self._notify_observers_of_creation()

    def can_have_as_cell(self, cell: "ICell") -> BoolExplained:
        super_result = super(InternalInput, self).can_have_as_cell(cell)
        if not super_result:
            return super_result
        if not isinstance(cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell):
            return FalseExplained(f"An InternalInput can only be created on an instance of ICompositeCell")
        return TrueExplained()

    def set_value(self, value: T) -> None:
        self.logger.debug(f"{self}.set_value( {value} ) waiting for prev value to be acknowledged @ InternalInput")
        while not self.__value_is_acknowledged.acquire(timeout=self.PULL_TIMEOUT):
            self.logger.warning(f"{self}.set_value() waiting... @ InternalInput level")
            if not self._is_deployed():
                raise NotDeployedException(f"{self} is set while not deployed")
        super(InternalInput, self)._set_value(value)

    def _clear_value(self) -> None:
        super(InternalInput, self)._clear_value()
        try:
            self.__value_is_acknowledged.release()
        except ValueError:
            # The value was not set...
            pass

    def clear_value(self) -> None:
        self._clear_value()

    def pull(self) -> T:
        self.logger.debug(f"{self}.pull() @ InternalInput level")
        if not self.value_is_set():
            self._wait_for_value(timeout=self.PULL_TIMEOUT)
        self.logger.debug(f"{self}.pull() got value @ InternalInput level")
        value = self.get_value()
        self.__exit_point._notify_new_value()
        self._acknowledge_value()
        return value

    def is_provided(self) -> bool:
        # Has no more info on whether it will be provided or not.
        # (It will always be provided with a value, but this value may be None in case of unconnected scalable
        #  cell inputs -> differentiate?)
        # -> It is required to be provided, otherwise a CloneCell would not be deployable.
        return True

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        return ()

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        return False

    def get_nb_incoming_connections(self) -> int:
        return 0

    def pull_as_connection(self, connection: "IConnection[T]") -> T:
        return self.__exit_point.pull_as_connection(connection)

    def all_outgoing_connections_have_pulled(self) -> bool:
        return self.__exit_point.have_all_outgoing_connections_pulled()

    def has_seen_value(self, connection: "IConnection[T]") -> bool:
        return self.__exit_point.has_seen_value(connection)

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        """
        Returns:
            The outgoing connections of this internal input.
        """
        return self.__exit_point.get_outgoing_connections()

    @classmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> BoolExplained:
        """
        Args:
            connection: connection to validate.
        Returns:
            TrueExplained if the given connection is a valid outgoing connection for this internal input.
            FalseExplained otherwise.
        """
        return ConnectionExitPoint.can_have_as_outgoing_connection(connection)

    def can_have_as_nb_outgoing_connections(self, number_of_outgoing_connections: int) -> BoolExplained:
        """
        Args:
            number_of_outgoing_connections: the number of outgoing connections to validate.
        Returns:
            TrueExplained if the given number is a valid number of outgoing connection for this internal input.
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

    def get_nb_available_pulls(self) -> Optional[int]:
        raise Exception("Not supported by this class?")

    def _get_connection_entry_point(self) -> Optional[ConnectionEntryPoint]:
        return None

    def _get_connection_exit_point(self) -> ConnectionExitPoint:
        return self.__exit_point

    def assert_is_valid(self) -> None:
        super(InternalInput, self).assert_is_valid()
        self.__exit_point.assert_is_valid()

    def delete(self) -> None:
        """
        Deletes this IO, and all its internals.

        Main mutator in the IO-ICell relation, as owner of the IO.

        Raises:
            CannotBeDeletedException
        """
        super(InternalInput, self).delete()
        self.__exit_point.delete()

    def __getstate__(self) -> Dict:
        # called during pickling
        new_state = super(InternalInput, self).__getstate__()
        new_state["_InternalInput__value_is_acknowledged"] = None
        return new_state

    def __setstate__(self, state: Dict) -> None:
        # called during unpickling
        super(InternalInput, self).__setstate__(state)
        self.__value_is_acknowledged = BoundedSemaphore(1)