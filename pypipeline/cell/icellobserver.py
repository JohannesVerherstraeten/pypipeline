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

from typing import Optional, Sequence
from abc import ABC, abstractmethod

from pypipeline.validation import BoolExplained


class IObservable(ABC):
    """
    Observer pattern interface, complementary to IObserver.

    Controlled class in the IObserver-IObservable relation.

    An observable notifies its observers about all its events. Therefore, it should never have the same observer
    registered multiple times.
    """

    @abstractmethod
    def get_observers(self) -> Sequence["IObserver"]:
        """
        Returns:
            The observers of this observable.
        """
        raise NotImplementedError

    @abstractmethod
    def _add_observer(self, observer: "IObserver") -> None:
        """
        Auxiliary mutator in the IObserver-IObservable relation, as observable.
        -> Should only be used by IObserver instances when registering this as observable.

        Args:
            observer: the observer to add to this observable.
        Raises:
            InvalidInputException
        """
        raise NotImplementedError

    @abstractmethod
    def _remove_observer(self, observer: "IObserver") -> None:
        """
        Auxiliary mutator in the IObserver-IObservable relation, as observable.
        -> Should only be used by IObserver instances when unregistering this as observable.

        Args:
            observer: the observer to remove from this observable.
        Raises:
            InvalidInputException
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def can_have_as_observer(cls, observer: "IObserver") -> "BoolExplained":
        """
        Args:
            observer: observer to validate.
        Returns:
            TrueExplained if the given observer is a valid observer for this observable. FalseExplained otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def has_as_observer(self, observer: "IObserver") -> bool:
        """
        Args:
            observer: an observer object.
        Returns:
            True if this observable has the given observer as observer, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def assert_has_proper_observers(self) -> None:
        """
        Raises:
            InvalidStateException: if any of the observers is in invalid state.
        """
        raise NotImplementedError

    @abstractmethod
    def notify_observers(self, event: "Event") -> None:
        """
        Calls the observer.update(event) method for every observer.

        Args:
            event: the event to notify the observers about.

        TODO may raise exceptions
        """
        raise NotImplementedError


class IObserver(ABC):
    """
    Observer pattern interface, complementary to IObservable.

    Controlling class in the IObserver-IObservable relation.
    This means that the IObserver class needs to make sure that all mutual references are set/cleared/validated when
    creating/removing the relation. For this, the IObserver can make use of the auxiliary mutators/validators from the
    IObservable interface.
    More specifically, the IObserver must, for every observable relation, implement an equivalent for the following
    main mutators/validators:
     - self.add_observable(observable)
     - self.remove_observable(observable)
     - self.can_have_as_observable(observable)
     - self.assert_has_proper_observables()
    """

    @abstractmethod
    def update(self, event: "Event") -> None:
        """
        Will be called by the observables of this observer.

        Args:
            event: the event to notify this observer about.

        # TODO may raise exceptions
        """
        raise NotImplementedError

    @abstractmethod
    def has_as_observable(self, observable: "IObservable") -> bool:
        """
        Args:
            observable: an observable object.
        Returns:
            True if this observer is observing the given observable.
        """
        raise NotImplementedError


class Event(ABC):

    def __init__(self, initiator: IObservable, debug_message: Optional[str] = None):
        self.__initiator = initiator
        self.__debug_message = debug_message

    def get_initiator(self) -> IObservable:
        return self.__initiator

    def __str__(self) -> str:
        debug_message_str = f", debug_message={self.__debug_message}" if self.__debug_message is not None else ""
        return f"{self.__class__.__name__}(initiator={self.get_initiator()}{debug_message_str})"


class ScalingStrategyUpdateEvent(Event):
    """Happens when scaling strategy of a scalable cell is updated."""
    pass


class CloneTypeUpdateEvent(Event):
    """Happens when a clone type is added to or removed from a scalable cell."""
    pass


class ParameterUpdateEvent(Event):
    """Happens when a new parameter value is updated."""
    pass


class IOCreatedEvent(Event):
    """Happens when a new input or output is created on a cell."""
    pass


class CloneCreatedEvent(Event):
    """Happens when a new clone is created."""
    pass
