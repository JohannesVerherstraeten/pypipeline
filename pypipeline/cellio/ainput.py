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

from typing import TypeVar, Generic, Optional, TYPE_CHECKING, Callable, Sequence, Dict
from threading import RLock

from pypipeline.cellio.acellio import AbstractIO
from pypipeline.cellio.icellio import IInput

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio.connectionendpoint import ConnectionEntryPoint, ConnectionExitPoint


T = TypeVar('T')


class AInput(AbstractIO[T], IInput[T], Generic[T]):

    def __init__(self, cell: "ICell", name: str, validation_fn: Optional[Callable[[T], bool]] = None):
        super(AInput, self).__init__(cell, name, validation_fn)
        self.__cell_pull_lock = RLock()

    def _get_cell_pull_lock(self) -> "RLock":
        return self.__cell_pull_lock

    def is_provided(self) -> bool:
        """Is this input provided by a default value / incoming connection or another way of value provision. """
        raise NotImplementedError

    def pull(self) -> T:
        raise NotImplementedError

    def reset(self) -> None:    # TODO threadsafety?
        self.logger.debug(f"{self} reset acquire... @ AInput")
        with self._get_state_lock():
            super(AInput, self).reset()
            for incoming_connection in self.get_incoming_connections():
                incoming_connection.reset()
        self.logger.debug(f"{self} reset release @ AInput")

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

    def get_nb_available_pulls(self) -> Optional[int]:
        raise NotImplementedError

    def _get_connection_entry_point(self) -> Optional["ConnectionEntryPoint"]:
        raise NotImplementedError

    def _get_connection_exit_point(self) -> Optional["ConnectionExitPoint"]:
        raise NotImplementedError

    def __getstate__(self) -> Dict:
        # called during pickling
        new_state = super(AInput, self).__getstate__()
        new_state["_AInput__cell_pull_lock"] = None
        return new_state

    def __setstate__(self, state: Dict) -> None:
        # called during unpickling
        self.__cell_pull_lock = RLock()
        super(AInput, self).__setstate__(state)
