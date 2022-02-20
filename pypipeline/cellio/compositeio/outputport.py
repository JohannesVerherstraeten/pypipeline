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
from pypipeline.cellio.connectionendpoint import ConnectionExitPoint, ConnectionEntryPoint, RecurrentConnectionExitPoint
from pypipeline.validation import BoolExplained, FalseExplained, TrueExplained
from pypipeline.exceptions import NoOutputProvidedException

if TYPE_CHECKING:
    from pypipeline.cell import ICell, ICompositeCell
    from pypipeline.connection import IConnection


T = TypeVar('T')


class OutputPort(AOutput[T], IConnectionExitPoint[T], IConnectionEntryPoint[T], Generic[T]):
    """
    OutputPort class.

    An output port is a type of output that can only be created for a composite cell.
    It accepts 1 incoming (internal) connection and infinite outgoing connections.
    Every time an output port is pulled, it will pull its cell (not its incoming connection). The cell has to
    make sure the output port value is set correctly.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    An IConnectionExitPoint is the controlled class in the IConnection-IConnectionExitPoint relation, as the
    source of the connection.

    TODO assert no recurrent connection can enter an OutputPort.
    """

    def __init__(self, cell: "ICompositeCell", name: str, initial_value: Optional[T] = None):
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
            initial_value: an optional initial value, which allows recurrent connections to be made from this
                output port.
        """
        super(OutputPort, self).__init__(cell, name)
        self.__entry_point: ConnectionEntryPoint[T] = ConnectionEntryPoint(self, max_incoming_connections=1)
        self.__exit_point: RecurrentConnectionExitPoint[T] = RecurrentConnectionExitPoint(self,
                                                                                          max_outgoing_connections=9999,
                                                                                          initial_value=initial_value)
        self._notify_observers_of_creation()

    def can_have_as_cell(self, cell: "ICell") -> BoolExplained:
        super_result = super(OutputPort, self).can_have_as_cell(cell)
        if not super_result:
            return super_result
        if not isinstance(cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell):
            return FalseExplained(f"An OutputPort can only be created on an instance of ICompositeCell")
        return TrueExplained()

    def pull(self) -> T:
        # self.logger.debug(f"{self}.pull()")
        self.get_cell().pull_as_output(self)  # should set output_value
        if not self.value_is_set():
            raise NoOutputProvidedException(f"Cell {self.get_cell()} didn't set output value for CellOutput {self} during pull")
        value: T = self.get_value()
        return value

    def set_value(self, value: T) -> None:
        super(OutputPort, self).set_value(value)
        return self.__exit_point._notify_new_value()

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
        return self.get_cell().get_nb_available_pulls()

    def _get_connection_entry_point(self) -> ConnectionEntryPoint:
        return self.__entry_point

    def _get_connection_exit_point(self) -> RecurrentConnectionExitPoint:
        return self.__exit_point

    def assert_is_valid(self) -> None:
        super(OutputPort, self).assert_is_valid()
        self.__entry_point.assert_is_valid()
        self.__exit_point.assert_is_valid()

    def delete(self) -> None:
        super(OutputPort, self).delete()
        self.__entry_point.delete()
        self.__exit_point.delete()
