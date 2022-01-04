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

from typing import TypeVar, Generic, Optional, Dict, TYPE_CHECKING, Any, Callable, Sequence
from threading import RLock, Condition
import logging

import pypipeline
from pypipeline.cell.icellobserver import IOCreatedEvent
from pypipeline.cellio.icellio import IO
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not
from pypipeline.exceptions import NoInputProvidedException, InvalidInputException, InvalidStateException, \
    NotDeployedException, CannotBeDeletedException

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio.connectionendpoint import ConnectionEntryPoint, ConnectionExitPoint


T = TypeVar('T')


class AbstractIO(IO[T], Generic[T]):
    """
    Abstract cell IO class.

    An IO is owned by its cell.

    An IO is the controlling class in the IO-ICell relation, as IO of the cell.
    -> Main mutators can be found in __init__ and delete()
    """

    def __init__(self, cell: "ICell", name: str, validation_fn: Optional[Callable[[T], bool]] = None):
        """
        Args:
            cell: the cell of which this IO will be part.
            name: the name of this IO. Should be unique within the cell.
            validation_fn: An optional validation function that will be used to validate every value that passes
                through this IO.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        raise_if_not(self.can_have_as_name(name), InvalidInputException)
        self.__name = name

        # Main mutator in the IO-ICell relation, as IO of the cell.
        raise_if_not(self.can_have_as_cell(cell), InvalidInputException)
        cell._add_io(self)      # Access to protected method on purpose     # May raise exceptions
        self.__cell = cell

        raise_if_not(self.can_have_as_validation_fn(validation_fn), InvalidInputException)
        self.__validation_fn = validation_fn

        self.__state_lock = RLock()
        self.__value: T = None   # type: ignore
        self.__value_is_set: bool = False
        self.__value_is_set_signal = Condition(self.__state_lock)
        self.__is_deployed: bool = False

    def _notify_observers_of_creation(self) -> None:
        """Should be called after every IO object has been fully created. """
        event = IOCreatedEvent(self.get_cell(), debug_message=f"io created with name {self.__name}")
        self.get_cell().notify_observers(event)

    def _get_cell_pull_lock(self) -> "RLock":
        """
        Returns:
            The pull lock which makes sure that the cell of this IO is not pulled concurrently.
        """
        raise NotImplementedError

    def _get_state_lock(self):
        """
        Returns:
            The lock which makes sure that this IO is not pulled concurrently.
        """
        return self.__state_lock

    def _get_value_is_set_signal(self):
        """
        Returns:
            A condition that gets signalled when a new value is available.
        """
        return self.__value_is_set_signal

    def get_name(self) -> str:
        return self.__name

    @classmethod
    def can_have_as_name(cls, name: str) -> BoolExplained:
        if not isinstance(name, str):
            return FalseExplained(f"Name should be a string, got {name}")
        elif len(name) == 0:
            return FalseExplained(f"Name should not be an empty string")
        elif "." in name:
            return FalseExplained(f"Name should not contain `.`")
        return TrueExplained()

    def get_full_name(self) -> str:
        return f"{self.get_cell().get_full_name()}.{self.get_name()}"

    def assert_has_proper_name(self) -> None:
        raise_if_not(self.can_have_as_name(self.get_name()), InvalidStateException, f"{self} has invalid name: ")

    def get_cell(self) -> "ICell":
        return self.__cell

    def can_have_as_cell(self, cell: "ICell") -> BoolExplained:
        if not isinstance(cell, pypipeline.cell.icell.ICell):
            return FalseExplained(f"Cell should be instance of ICell, got {cell}")
        return TrueExplained()

    def assert_has_proper_cell(self) -> None:
        cell = self.get_cell()
        raise_if_not(self.can_have_as_cell(cell), InvalidStateException, f"{self} has invalid cell: ")
        if not cell.has_as_io(self):
            raise InvalidStateException(f"Inconsistent relation: {cell} doesn't have {self} as io")

    def get_validation_fn(self) -> Optional[Callable[[T], bool]]:
        return self.__validation_fn

    def can_have_as_validation_fn(self, validation_fn: Optional[Callable[[T], bool]]) -> BoolExplained:
        if validation_fn is None:
            return TrueExplained()
        if not isinstance(validation_fn, Callable):
            return FalseExplained(f"The validation function should be a callable that accepts one argument of type T, "
                                  f"and returns a boolean.")
        return TrueExplained()

    def assert_has_proper_validation_fn(self) -> None:
        validation_fn = self.get_validation_fn()
        raise_if_not(self.can_have_as_validation_fn(validation_fn), InvalidStateException,
                     f"{self} has invalid validation function: ")

    def get_all_connections(self) -> "Sequence[IConnection[T]]":
        result = list(self.get_incoming_connections())
        result.extend(self.get_outgoing_connections())
        return result

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

    def _get_connection_entry_point(self) -> Optional["ConnectionEntryPoint"]:
        raise NotImplementedError

    def _get_connection_exit_point(self) -> Optional["ConnectionExitPoint"]:
        raise NotImplementedError

    def _deploy(self) -> None:
        assert not self._is_deployed()
        for connection in self.get_all_connections():
            if connection.get_source()._is_deployed() or connection.get_target()._is_deployed():
                connection._deploy()  # access to protected member on purpose
        self.__is_deployed = True

    def _undeploy(self) -> None:
        assert self._is_deployed()
        for connection in self.get_all_connections():
            if connection.get_source()._is_deployed() and connection.get_target()._is_deployed():
                connection._undeploy()  # access to protected member on purpose
        self.__is_deployed = False

    def _is_deployed(self) -> bool:
        return self.__is_deployed

    def _assert_is_properly_deployed(self) -> None:
        for connection in self.get_all_connections():
            connection._assert_is_properly_deployed()        # access to protected member on purpose

    def pull(self) -> T:
        raise NotImplementedError

    def reset(self) -> None:
        self._clear_value()

    def get_value(self) -> T:
        with self._get_state_lock():
            if not self.__value_is_set:
                raise NoInputProvidedException(f"{self}.get_value() called, but value has not yet been set.")
            return self.__value

    def _set_value(self, value: T) -> None:
        if not self.can_have_as_value(value):
            raise InvalidInputException(f"{self}: Invalid value: {value}")
        with self._get_state_lock():
            self.logger.debug(f"{self}.set_value( {value} ) @ AbstractIO")
            self.__value = value
            self.__value_is_set = True
            self.__value_is_set_signal.notify()
            exit_point = self._get_connection_exit_point()
            if exit_point is not None:
                exit_point._notify_new_value()

    def _clear_value(self) -> None:
        with self._get_state_lock():
            self.__value_is_set = False

    def value_is_set(self) -> bool:
        with self._get_state_lock():
            return self.__value_is_set

    def _wait_for_value(self, interruption_frequency: Optional[float] = None) -> None:
        with self._get_state_lock():
            while not self.__value_is_set_signal.wait(timeout=interruption_frequency):
                self.logger.warning(f"{self}.wait_for_value() waiting... @ AbstractIO level")
                if not self._is_deployed():
                    raise NotDeployedException(f"{self} got undeployed while someone was waiting for the value. ")

    def _acknowledge_value(self) -> None:
        self._clear_value()

    def can_have_as_value(self, value: T) -> bool:
        if self.__validation_fn is not None:
            return self.__validation_fn(value)
        return True

    def assert_has_proper_value(self) -> None:
        if self.value_is_set() and not self.can_have_as_value(self.get_value()):
            raise InvalidStateException(f"{self} has an invalid value: {self.get_value()}")

    def _get_sync_state(self) -> Dict[str, Any]:
        with self._get_state_lock():
            state: Dict[str, Any] = dict()
            return state

    def _set_sync_state(self, state: Dict) -> None:
        pass

    def get_nb_available_pulls(self) -> Optional[int]:
        raise NotImplementedError

    def _is_optional_even_when_typing_says_otherwise(self) -> bool:
        if len(self.get_outgoing_connections()) == 0:
            return False
        is_optional = True
        for conn in self.get_outgoing_connections():
            target = conn.get_target()
            if not target._is_optional_even_when_typing_says_otherwise():
                is_optional = False
                break
        return is_optional

    def get_topology_description(self) -> Dict[str, Any]:
        result = {
            "io": str(self),
            "type": self.__class__.__name__,
            "name": self.get_name(),
            "id": self.get_full_name(),
        }
        return result

    def assert_is_valid(self) -> None:
        self.assert_has_proper_name()
        self.assert_has_proper_cell()
        self.assert_has_proper_validation_fn()
        self.assert_has_proper_value()
        self._assert_is_properly_deployed()

    def delete(self) -> None:
        if self._is_deployed():
            raise CannotBeDeletedException(f"{self} cannot be deleted while it is deployed.")
        if self.get_nb_incoming_connections() > 0:
            raise CannotBeDeletedException(f"Cannot delete {self}, as it still has {self.get_nb_incoming_connections()}"
                                           f"incoming connections.")
        if self.get_nb_outgoing_connections() > 0:
            raise CannotBeDeletedException(f"Cannot delete {self}, as it still has {self.get_nb_outgoing_connections()}"
                                           f"outgoing connections.")
        # Main mutator in the IO-ICell relation, as owner of the IO.
        self.__cell._remove_io(self)        # Access to protected method on purpose
        self.__cell = None

    def __getstate__(self) -> Dict:
        # called during pickling
        new_state = dict(self.__dict__)
        new_state["_AbstractIO__state_lock"] = None
        new_state["_AbstractIO__value_is_set_signal"] = None
        return new_state

    def __setstate__(self, state: Dict) -> None:
        # called during unpickling
        self.__dict__ = state
        self.__state_lock = RLock()
        self.__value_is_set_signal = Condition(self.__state_lock)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.get_full_name()})"
