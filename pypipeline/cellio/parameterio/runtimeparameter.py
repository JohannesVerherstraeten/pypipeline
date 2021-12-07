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

from typing import TypeVar, Generic, TYPE_CHECKING, Dict, Any, Optional, Callable

from pypipeline.cell.icellobserver import ParameterUpdateEvent
from pypipeline.cellio.standardio import Input
from pypipeline.exceptions import InvalidInputException, NoInputProvidedException, \
    InvalidStateException

if TYPE_CHECKING:
    from pypipeline.cell import ICell

T = TypeVar('T')


class RuntimeParameter(Input[T], Generic[T]):
    """
    Runtime parameter class.

    A runtime parameter is a type of input that accepts 1 incoming connection and no outgoing connections.
    Every time a runtime parameter is pulled, it will pull the incoming connection, if available.
    If no incoming connection is present, it returns the default value.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    An IConnectionEntryPoint is the controlled class in the IConnection-IConnectionEntryPoint relation, as the
    target of the connection.
    """

    __DEFAULT_VALUE_KEY: str = "default_value"

    def __init__(self,
                 cell: "ICell",
                 name: str,
                 validation_fn: Optional[Callable[[T], bool]] = None):
        super(RuntimeParameter, self).__init__(cell, name, validation_fn)
        self.__default_value: T = None   # type: ignore
        self.__default_value_is_set: bool = False

    def set_default_value(self, value: T) -> None:
        if not self.can_have_as_value(value):
            raise InvalidInputException(f"{self}: Invalid value: {value}")
        with self._get_state_lock():
            self.logger.debug(f"{self}.set_default_value( {value} ) @ RuntimeParameter")
            self.__default_value = value
            self.__default_value_is_set = True
        event = ParameterUpdateEvent(self.get_cell())       # TODO avoid indirection of cell
        self.get_cell().notify_observers(event)

    def get_default_value(self) -> T:
        with self._get_state_lock():
            if not self.__default_value_is_set:
                return NoInputProvidedException(f"{self}.get_default_value() called, but default value has not yet "
                                                f"been set.")
            return self.__default_value

    def default_value_is_set(self) -> bool:
        with self._get_state_lock():
            return self.__default_value_is_set

    def _clear_default_value(self) -> None:
        with self._get_state_lock():
            self.__default_value_is_set = False

    def assert_has_proper_default_value(self) -> None:
        if self.default_value_is_set() and not self.can_have_as_value(self.get_default_value()):
            raise InvalidStateException(f"{self} has an invalid default value: {self.get_default_value()}")

    def pull(self) -> T:
        if self.get_nb_incoming_connections() == 0:
            return self.get_default_value()
        result = super(RuntimeParameter, self).pull()
        if result is None:
            return self.get_default_value()
        return result

    def set_value(self, value: T) -> None:
        """
        Same as self.set_default_value().

        Note: this method is not related to self._set_value(value) which is used by incoming connections to set the
        (not-default) value of this RuntimeParameter.
        """
        self.set_default_value(value)

    def is_provided(self) -> bool:
        if super(RuntimeParameter, self).is_provided():
            return True
        else:
            return self.default_value_is_set()

    def _is_optional_even_when_typing_says_otherwise(self) -> bool:
        return True    # A RuntimeParameter can handle None values being set: it will return the default value instead

    def _get_sync_state(self) -> Dict[str, Any]:
        with self._get_state_lock():
            state: Dict[str, Any] = super(RuntimeParameter, self)._get_sync_state()
            state[self.__DEFAULT_VALUE_KEY] = self.get_default_value() if self.default_value_is_set() else None
            return state

    def _set_sync_state(self, state: Dict) -> None:
        with self._get_state_lock():
            super(RuntimeParameter, self)._set_sync_state(state)
            if state[self.__DEFAULT_VALUE_KEY] is not None:
                self.set_default_value(state[self.__DEFAULT_VALUE_KEY])

    def get_nb_available_pulls(self) -> Optional[int]:
        if self.get_nb_incoming_connections() == 0:
            return None
        return super(RuntimeParameter, self).get_nb_available_pulls()

    def assert_is_valid(self) -> None:
        super(RuntimeParameter, self).assert_is_valid()
        self.assert_has_proper_default_value()
