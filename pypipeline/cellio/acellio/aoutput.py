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

from typing import TypeVar, Generic, Optional, TYPE_CHECKING, Callable, Sequence
from threading import RLock

from pypipeline.cellio.acellio.abstractio import AbstractIO
from pypipeline.cellio.icellio import IOutput

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio.connectionendpoint import ConnectionEntryPoint, ConnectionExitPoint


T = TypeVar('T')


class AOutput(AbstractIO[T], IOutput[T], Generic[T]):

    def __init__(self, cell: "ICell", name: str, validation_fn: Optional[Callable[[T], bool]] = None):
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
            validation_fn: An optional validation function that will be used to validate every value that passes
                through this IO.
        """
        super(AOutput, self).__init__(cell, name, validation_fn)
        self.__reset_is_busy: bool = False

    def _get_cell_pull_lock(self) -> "RLock":
        return self.get_cell()._get_pull_lock()

    def pull(self) -> T:
        raise NotImplementedError

    def reset(self) -> None:    # TODO threadsafety?
        if self.__reset_is_busy:
            # recurrently called
            return
        with self._get_state_lock():
            self.__reset_is_busy = True
            super(AOutput, self).reset()
            self.get_cell().reset()
            self.__reset_is_busy = False

    def set_value(self, value: T) -> None:
        super(AOutput, self)._set_value(value)

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        raise NotImplementedError

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        raise NotImplementedError

    def get_nb_incoming_connections(self) -> int:
        raise NotImplementedError

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        raise NotImplementedError

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        raise NotImplementedError

    def get_nb_outgoing_connections(self) -> int:
        raise NotImplementedError

    def all_outgoing_connections_have_pulled(self) -> bool:
        raise NotImplementedError

    def get_nb_available_pulls(self) -> Optional[int]:
        raise NotImplementedError

    def _get_connection_entry_point(self) -> Optional["ConnectionEntryPoint"]:
        raise NotImplementedError

    def _get_connection_exit_point(self) -> Optional["ConnectionExitPoint"]:
        raise NotImplementedError
