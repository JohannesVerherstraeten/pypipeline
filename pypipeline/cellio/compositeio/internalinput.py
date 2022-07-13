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

from typing import TypeVar, Generic, TYPE_CHECKING, Optional, Dict, Sequence
from threading import BoundedSemaphore

import pypipeline
from pypipeline.cellio.acellio.ainput import AInput
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
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
        """
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

    def get_total_pull_duration_since_last_read(self) -> float:
        # Internal inputs don't pull any input value
        return 0.

    def _on_pull(self) -> T:
        self.logger.debug(f"{self}.pull() @ InternalInput level")
        if not self.value_is_set():
            self._wait_for_value(interruption_frequency=self.PULL_TIMEOUT)
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
        return self.__exit_point.get_outgoing_connections()

    @classmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> BoolExplained:
        return ConnectionExitPoint.can_have_as_outgoing_connection(connection)

    def can_have_as_nb_outgoing_connections(self, number_of_outgoing_connections: int) -> BoolExplained:
        return self.__exit_point.can_have_as_nb_outgoing_connections(number_of_outgoing_connections)

    def _add_outgoing_connection(self, connection: "IConnection[T]") -> None:
        self.__exit_point._add_outgoing_connection(connection)

    def _remove_outgoing_connection(self, connection: "IConnection[T]") -> None:
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
