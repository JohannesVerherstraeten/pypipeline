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

from typing import TypeVar, Generic, Optional, TYPE_CHECKING, Sequence, Callable

from pypipeline.cellio.acellio.ainput import AInput
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.cellio.connectionendpoint import ConnectionEntryPoint
from pypipeline.exceptions import UnconnectedException
from pypipeline.validation import BoolExplained

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio.connectionendpoint import ConnectionExitPoint

T = TypeVar('T')


class Input(AInput[T], IConnectionEntryPoint[T], Generic[T]):
    """
    Input class.

    An Input is a type of input that accepts 1 incoming connection and no outgoing connections.
    Every time an Input is pulled, it will pull the incoming connection. If no incoming connection is present,
    an UnconnectedException is raised.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    """

    def __init__(self, cell: "ICell", name: str, validation_fn: Optional[Callable[[T], bool]] = None):
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
            validation_fn: An optional validation function that will be used to validate every value that passes
                through this IO.
        """
        super(Input, self).__init__(cell, name, validation_fn)
        self.__entry_point: ConnectionEntryPoint[T] = ConnectionEntryPoint(self, max_incoming_connections=1)
        self._notify_observers_of_creation()

    def pull(self) -> T:
        if self.get_nb_incoming_connections() == 0:
            raise UnconnectedException(f"CellInput {self} has no incoming connection to pull")
        connection = self.get_incoming_connections()[0]
        result: T = connection.pull()
        self._set_value(result)
        return result

    def is_provided(self) -> bool:
        return self.get_nb_incoming_connections() > 0

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        return ()

    def has_as_outgoing_connection(self, connection: "IConnection[T]") -> bool:
        return False

    def get_nb_outgoing_connections(self) -> int:
        return 0

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        return self.__entry_point.get_incoming_connections()

    def can_have_as_incoming_connection(self, connection: "IConnection[T]") -> BoolExplained:
        return self.__entry_point.can_have_as_incoming_connection(connection)

    def can_have_as_nb_incoming_connections(self, number_of_incoming_connections: int) -> BoolExplained:
        return self.__entry_point.can_have_as_nb_incoming_connections(number_of_incoming_connections)

    def _add_incoming_connection(self, connection: "IConnection[T]") -> None:
        self.__entry_point._add_incoming_connection(connection)     # access to protected method on purpose

    def _remove_incoming_connection(self, connection: "IConnection[T]") -> None:
        self.__entry_point._remove_incoming_connection(connection)  # access to protected method on purpose

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        return self.__entry_point.has_as_incoming_connection(connection)

    def get_max_nb_incoming_connections(self) -> int:
        return self.__entry_point.get_max_nb_incoming_connections()

    def get_nb_incoming_connections(self) -> int:
        return self.__entry_point.get_nb_incoming_connections()

    def has_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> bool:
        return self.__entry_point.has_incoming_connection_with(source)

    def get_incoming_connection_with(self, source: "IConnectionExitPoint[T]") -> "IConnection[T]":
        return self.__entry_point.get_incoming_connection_with(source)

    def assert_has_proper_incoming_connections(self) -> None:
        self.__entry_point.assert_has_proper_incoming_connections()

    def _get_connection_entry_point(self) -> ConnectionEntryPoint:
        return self.__entry_point

    def _get_connection_exit_point(self) -> Optional["ConnectionExitPoint"]:
        return None

    def get_nb_available_pulls(self) -> Optional[int]:
        incoming_connections = self.get_incoming_connections()
        if len(incoming_connections) == 0:
            raise UnconnectedException(f"{str(self)} has no incoming connections. ")
        return incoming_connections[0].get_nb_available_pulls()

    def assert_is_valid(self) -> None:
        super(Input, self).assert_is_valid()
        self.__entry_point.assert_is_valid()

    def delete(self) -> None:
        super(Input, self).delete()
        self.__entry_point.delete()
