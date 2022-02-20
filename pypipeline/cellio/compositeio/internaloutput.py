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

from typing import TypeVar, Generic, TYPE_CHECKING, Sequence, Optional

import pypipeline
from pypipeline.cellio.acellio.aoutput import AOutput
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.cellio.connectionendpoint import ConnectionExitPoint, ConnectionEntryPoint
from pypipeline.validation import BoolExplained, FalseExplained, TrueExplained
from pypipeline.exceptions import UnconnectedException

if TYPE_CHECKING:
    from pypipeline.cell import ICell, ICompositeCell
    from pypipeline.connection import IConnection


T = TypeVar('T')


class InternalOutput(AOutput[T], IConnectionEntryPoint[T], Generic[T]):
    """
    InternalOutput class.

    An internal output is a type of output that can only be created on a composite cell.
    It accepts 1 incoming (internal) connection and no outgoing connections.
    Every time an internal output is pulled, it will pull its cell (not its incoming connection). The cell has to
    make sure the internal output value is set correctly.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.

    TODO assert no recurrent connection can enter an OutputPort.
    """

    def __init__(self, cell: "ICompositeCell", name: str):
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
        """
        super(InternalOutput, self).__init__(cell, name)
        self.__entry_point: ConnectionEntryPoint[T] = ConnectionEntryPoint(self, max_incoming_connections=1)
        self._notify_observers_of_creation()

    def can_have_as_cell(self, cell: "ICell") -> BoolExplained:
        super_result = super(InternalOutput, self).can_have_as_cell(cell)
        if not super_result:
            return super_result
        if not isinstance(cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell):
            return FalseExplained(f"An InternalOutput can only be created on an instance of ICompositeCell")
        return TrueExplained()

    def pull(self) -> T:
        if self.get_nb_incoming_connections() == 0:
            raise UnconnectedException(f"{self} has no incoming connection")
        connection = self.get_incoming_connections()[0]
        return connection.pull()

    def all_outgoing_connections_have_pulled(self) -> bool:
        return True

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
        self.__entry_point._add_incoming_connection(connection)

    def _remove_incoming_connection(self, connection: "IConnection[T]") -> None:
        self.__entry_point._remove_incoming_connection(connection)

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

    def get_nb_available_pulls(self) -> Optional[int]:
        incoming_connections = self.get_incoming_connections()
        if len(incoming_connections) == 0:
            raise UnconnectedException(f"{str(self)} has no incoming connections. ")
        return incoming_connections[0].get_nb_available_pulls()

    def _get_connection_entry_point(self) -> ConnectionEntryPoint:
        return self.__entry_point

    def _get_connection_exit_point(self) -> Optional[ConnectionExitPoint]:
        return None

    def assert_is_valid(self) -> None:
        super(InternalOutput, self).assert_is_valid()
        self.__entry_point.assert_is_valid()

    def delete(self) -> None:
        super(InternalOutput, self).delete()
        self.__entry_point.delete()
