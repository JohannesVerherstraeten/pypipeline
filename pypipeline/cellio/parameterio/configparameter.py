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

from typing import TypeVar, Generic, TYPE_CHECKING, Dict, Any, Optional, Sequence, Callable

from pypipeline.cell.icellobserver import ParameterUpdateEvent
from pypipeline.cellio.acellio.ainput import AInput

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio.connectionendpoint import ConnectionEntryPoint, ConnectionExitPoint

T = TypeVar('T')


class ConfigParameter(AInput[T], Generic[T]):
    """
    Config params should only be pulled/used during the deployment of a cell.
    """

    __VALUE_KEY: str = "value"

    def __init__(self, cell: "ICell", name: str, validation_fn: Optional[Callable[[T], bool]] = None):
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
            validation_fn: An optional validation function that will be used to validate every value that passes
                through this IO.
        """
        super(ConfigParameter, self).__init__(cell, name, validation_fn)
        self._notify_observers_of_creation()

    def pull(self) -> T:
        return self.get_value()

    def set_value(self, value: T) -> None:
        """
        Args:
            value: the new value for this configuration parameter.
        """
        super(ConfigParameter, self)._set_value(value)
        event = ParameterUpdateEvent(self.get_cell())       # TODO avoid indirection of cell
        self.get_cell().notify_observers(event)

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        return ()

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        return False

    def get_nb_incoming_connections(self) -> int:
        return 0

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        return ()

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        return False

    def get_nb_outgoing_connections(self) -> int:
        return 0

    def _get_connection_entry_point(self) -> Optional["ConnectionEntryPoint"]:
        return None

    def _get_connection_exit_point(self) -> Optional["ConnectionExitPoint"]:
        return None

    def is_provided(self) -> bool:
        return self.value_is_set()

    def get_nb_available_pulls(self) -> Optional[int]:
        return None

    def _get_sync_state(self) -> Dict[str, Any]:
        with self._get_state_lock():
            state: Dict[str, Any] = super(ConfigParameter, self)._get_sync_state()
            state[self.__VALUE_KEY] = self.get_value() if self.value_is_set() else None
            return state

    def _set_sync_state(self, state: Dict) -> None:
        with self._get_state_lock():
            super(ConfigParameter, self)._set_sync_state(state)
            if state[self.__VALUE_KEY] is not None:
                self.set_value(state[self.__VALUE_KEY])

    def reset(self) -> None:
        with self._get_state_lock():
            current_value = self.get_value()
            super(ConfigParameter, self).reset()
            self.set_value(current_value)
