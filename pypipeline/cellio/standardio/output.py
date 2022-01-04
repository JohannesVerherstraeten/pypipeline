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

from typing import TypeVar, Generic, Optional, TYPE_CHECKING, Sequence

from pypipeline.cellio.acellio.aoutput import AOutput
from pypipeline.cellio.icellio import IConnectionEntryPoint, IConnectionExitPoint
from pypipeline.cellio.connectionendpoint import RecurrentConnectionExitPoint
from pypipeline.validation import BoolExplained
from pypipeline.exceptions import NoOutputProvidedException

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio.connectionendpoint import ConnectionEntryPoint

T = TypeVar('T')


class Output(AOutput[T], IConnectionExitPoint[T], Generic[T]):
    """
    Output class.

    An Output is a type of output that accepts infinite outgoing connections.
    Every time an Output is pulled, it will pull its cell. If the cell doesn't provide an output value,
    a NoOutputProvidedException is raised.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionExitPoint is the controlled class in the IConnection-IConnectionExitPoint relation, as the
    source of the connection.
    """

    def __init__(self, cell: "ICell", name: str, initial_value: Optional[T] = None):
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
            initial_value: an optional initial value, which allows recurrent connections to be made from this
                connection exit point.
        """
        super(Output, self).__init__(cell, name, validation_fn=None)
        self.__exit_point: RecurrentConnectionExitPoint[T] = RecurrentConnectionExitPoint(self,
                                                                                          max_outgoing_connections=9999,
                                                                                          initial_value=initial_value)
        self._notify_observers_of_creation()

    def pull(self) -> T:
        # self.logger.debug(f"{self}.pull() -> {self.get_cell()}.pull_as_output()")
        self.get_cell().pull_as_output(self)  # should set output_value
        if not self.value_is_set():
            raise NoOutputProvidedException(f"Cell {self.get_cell()} didn't set output value for CellOutput {self} during pull")
        value: T = self.get_value()
        return value

    def set_value(self, value: T) -> None:
        super(Output, self).set_value(value)
        self.__exit_point._notify_new_value()        # access to protected member on purpose

    def get_incoming_connections(self) -> "Sequence[IConnection[T]]":
        return ()

    def get_nb_incoming_connections(self) -> int:
        return 0

    def has_as_incoming_connection(self, connection: "IConnection[T]") -> bool:
        return False

    def get_outgoing_connections(self) -> "Sequence[IConnection[T]]":
        return self.__exit_point.get_outgoing_connections()

    def pull_as_connection(self, connection: "IConnection[T]") -> T:
        return self.__exit_point.pull_as_connection(connection)

    def all_outgoing_connections_have_pulled(self) -> bool:
        return self.__exit_point.have_all_outgoing_connections_pulled()

    def has_seen_value(self, connection: "IConnection[T]") -> bool:
        return self.__exit_point.has_seen_value(connection)

    @classmethod
    def can_have_as_outgoing_connection(cls, connection: "IConnection[T]") -> BoolExplained:
        return RecurrentConnectionExitPoint.can_have_as_outgoing_connection(connection)

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

    def _get_connection_entry_point(self) -> Optional["ConnectionEntryPoint"]:
        return None

    def _get_connection_exit_point(self) -> RecurrentConnectionExitPoint:
        return self.__exit_point

    def assert_is_valid(self) -> None:
        super(Output, self).assert_is_valid()
        self.__exit_point.assert_is_valid()

    def delete(self) -> None:
        super(Output, self).delete()
        self.__exit_point.delete()
