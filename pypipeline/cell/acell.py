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

from typing import Optional, List, Any, TYPE_CHECKING, Dict, Type, Sequence
from threading import RLock, Condition
import logging
from pprint import pformat

import pypipeline
from pypipeline.cell.icellobserver import IObservable
from pypipeline.cell.icell import ICell
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not
from pypipeline.exceptions import NotDeployableException, InvalidInputException, \
    InvalidStateException, NotDeployedException, AlreadyDeployedException, CannotBeDeletedException

if TYPE_CHECKING:
    from pypipeline.cellio import IO, IInput, IOutput
    from pypipeline.cell.compositecell.icompositecell import ICompositeCell
    from pypipeline.cell.icellobserver import IObserver, Event


class ACell(ICell):
    """
    Abstract cell class.

    An ICell is owned by its (optional) parent cell.
    An ICell owns its IO.

    An ICell is the controlled class in the ICompositeCell-ICell relation, as internal cell of a parent composite class.
    An ICell is the controlled class in the IObserver-IObservable relation, as observable.

    An ICell is the controlled class in the IO-ICell relation, as owner of the IO.
    """

    __INPUT_STATES_KEY: str = "input_states"
    PULL_TIMEOUT: float = 5.0

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str) -> None:
        """
        Args:
            parent_cell: the cell in which this cell must be nested.
            name: name of this cell.
        Raises:
            InvalidInputException
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        raise_if_not(self.can_have_as_name(name), InvalidInputException)
        raise_if_not(self.can_have_as_parent_cell(parent_cell), InvalidInputException)

        self.__name: str = name
        self.__parent_cell: "Optional[ICompositeCell]" = None
        self.__inputs: Dict[str, "IInput"] = dict()
        self.__outputs: Dict[str, "IOutput"] = dict()
        self.__observers: List["IObserver"] = []
        self.__is_deployed: bool = False
        self.__topology_is_set: bool = False
        self.__has_been_pulled_at_least_once = False
        self.__pull_as_output_lock = Condition(RLock())
        self.__pull_lock = RLock()
        self.__reset_is_busy: bool = False

        if parent_cell is not None:
            parent_cell._add_internal_cell(self)        # access to protected method on purpose

    def _get_pull_lock(self) -> RLock:
        return self.__pull_lock

    # ------ Cell name ------

    def get_name(self) -> str:
        """
        Returns:
            The name of this cell.
        """
        return self.__name

    def get_full_name(self) -> str:
        """
        Returns:
            The name of this cell, preceded by the full name of the parent cell.
            Format: grand_parent_name.parent_name.cell_name
        """
        parent_cell = self.get_parent_cell()
        prefix = parent_cell.get_full_name() + "." if parent_cell is not None else ""
        return prefix + self.get_name()

    @classmethod
    def can_have_as_name(cls, name: str) -> BoolExplained:
        """
        Args:
            name: the name to validate.
        Returns:
            TrueExplained if the name is a valid name for this cell.
            FalseExplained otherwise.
        """
        if not isinstance(name, str):
            return FalseExplained(f"Name should be a string, got {name}")
        elif len(name) == 0:
            return FalseExplained(f"Name should not be an empty string")
        elif "." in name:
            return FalseExplained(f"Name should not contain `.`")
        return TrueExplained()

    def assert_has_proper_name(self) -> None:
        """
        Raises:
            InvalidStateException: if the name of this cell is invalid.
        """
        raise_if_not(self.can_have_as_name(self.get_name()), InvalidStateException, f"{self} has invalid name: ")

    # ------ Parent cell ------

    def get_parent_cell(self) -> "Optional[ICompositeCell]":
        """
        Returns:
            The parent cell of this cell, if it has one.
        """
        return self.__parent_cell

    def _set_parent_cell(self, parent_cell: "Optional[ICompositeCell]") -> None:
        """
        Auxiliary mutator in the ICompositeCell-ICell, as internal cell of a parent composite cell.
        -> Should only be used by ICompositeCell instances when registering this as internal cell.

        Args:
            parent_cell: the new parent cell.
        Raises:
            InvalidInputException
        """
        raise_if_not(self.can_have_as_parent_cell(parent_cell), InvalidInputException)
        if self.get_parent_cell() is not None and parent_cell is not None:
            raise InvalidInputException(f"{self} already has a parent cell: {self.get_parent_cell()}")
        self.__parent_cell = parent_cell

    @classmethod
    def can_have_as_parent_cell(cls, cell: "Optional[ICompositeCell]") -> BoolExplained:
        """
        Args:
            cell: the cell to validate.
        Returns:
            TrueExplained if the cell is a valid parent cell for this cell.
            FalseExplained otherwise.
        """
        if cell is None:
            return TrueExplained()
        if not isinstance(cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell):
            return FalseExplained(f"A cell's parent cell must be a subtype of ICompositeCell or None, "
                                  f"got {type(cell)}")
        return TrueExplained()

    def assert_has_proper_parent_cell(self) -> None:
        """
        Raises:
            InvalidStateException: if this cell doesn't have a valid parent cell.
        """
        parent_cell = self.get_parent_cell()
        raise_if_not(self.can_have_as_parent_cell(parent_cell), InvalidStateException,
                     f"{self} has an invalid parent cell: ")
        if parent_cell is not None and not parent_cell.has_as_internal_cell(self):
            raise InvalidStateException(f"Inconsistent relation: {parent_cell} doesn't have {self} as internal cell. ")

    # ------ General IO ------

    def get_all_io(self) -> Sequence["IO"]:
        """
        Returns:
            All inputs and outputs (IO) of this cell.
        """
        result: List["IO"] = list(self.__inputs.values())
        result.extend(self.__outputs.values())
        return result

    def get_io(self, name: str) -> "IO":
        """
        Args:
            name: the name of the cell's input/output to get.
        Returns:
            This cell's input/output with the given name.
        Raises:
            KeyError: this cell has no IO with the given name.
        """
        if name in self.__inputs:
            return self.__inputs[name]
        elif name in self.__outputs:
            return self.__outputs[name]
        else:
            raise KeyError(f"Cell {self} has no input or output with name {name}")

    def get_io_names(self) -> Sequence[str]:
        """
        Returns:
            The names of all IO of this cell.
        """
        return [io.get_name() for io in self.get_all_io()]

    def has_as_io(self, io: "IO") -> bool:
        """
        Args:
            io: an input/output (IO) object.
        Returns:
            True if the given IO is part of this cell, False otherwise.
        """
        return io in self.__inputs.values() or io in self.__outputs.values()

    def can_have_as_io(self, io: "IO") -> "BoolExplained":
        """
        Args:
            io: IO object (input or output) to validate.
        Returns:
            TrueExplained if the given IO is a valid IO for this cell. FalseExplained otherwise.
        """
        if not isinstance(io, pypipeline.cellio.icellio.IO):
            return FalseExplained(f"Cell IO must be of type IO, got {type(io)}")
        if not isinstance(io, (pypipeline.cellio.IInput, pypipeline.cellio.IOutput)):
            return FalseExplained(f"Cell IO must be either a subtype of IInput or IOutput, got {type(io)}")
        return TrueExplained()

    def _add_io(self, io: "IO") -> None:
        """
        Auxiliary mutator in the IO-ICell relation, as owner of the IO.
        -> Should only be used by IO instances in their __init__.

        Args:
            io: the IO (input/output) to add to this cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
        if self.is_deployed():
            raise AlreadyDeployedException(f"{self}: an input cannot be added when this cell is deployed.")
        raise_if_not(self.can_have_as_io(io), InvalidInputException)
        if io.get_name() in self.get_io_names():
            raise InvalidInputException(f"{self} already has an IO with name {io.get_name()}")
        if isinstance(io, pypipeline.cellio.IInput):
            self.__inputs[io.get_name()] = io
        elif isinstance(io, pypipeline.cellio.IOutput):
            self.__outputs[io.get_name()] = io
        else:
            assert False

    def _remove_io(self, io: "IO") -> None:
        """
        Auxiliary mutator in the IO-ICell relation, as owner of the IO.
        -> Should only be used by IO instances in their delete().

        Args:
            io: the IO (input/output) to remove from this cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
        if self.is_deployed():
            raise AlreadyDeployedException(f"{self}: an input cannot be removed when this cell is deployed.")
        if not self.has_as_io(io):
            raise InvalidInputException(f"{self}: doesn't have {io} as IO, so it cannot be removed.")
        if io.get_nb_incoming_connections() > 0 or io.get_nb_outgoing_connections() > 0:
            raise InvalidInputException(f"{self}: cannot remove {io} because it still has connections attached to it.")
        if isinstance(io, pypipeline.cellio.IInput):
            del self.__inputs[io.get_name()]
        elif isinstance(io, pypipeline.cellio.IOutput):
            del self.__outputs[io.get_name()]
        else:
            assert False

    def assert_has_proper_io(self) -> None:
        """
        Raises:
            InvalidStateException: if one of this cell's IO is invalid.
        """
        for io in self.get_all_io():
            raise_if_not(self.can_have_as_io(io), InvalidStateException)
            if io.get_cell() != self:
                raise InvalidStateException(f"Inconsistent relation: {io} doesn't have {self} as cell.")
            io.assert_is_valid()

    # ------ Inputs ------

    def get_inputs(self) -> Sequence["IInput"]:
        """
        Returns:
            All inputs of this cell.
        """
        return tuple(self.__inputs.values())

    def get_input_names(self) -> Sequence[str]:
        """
        Returns:
            The names of all inputs of this cell.
        """
        return tuple(self.__inputs.keys())

    def get_input(self, name: str) -> "IInput":
        """
        Raises:
            KeyError: this cell has no input with the given name.
        """
        return self.__inputs[name]

    def get_inputs_recursively(self) -> Sequence["IInput"]:
        """
        Returns:
            All inputs of this cell, and those of its internal cells.
        """
        return self.get_inputs()

    def get_input_recursively(self, full_name: str) -> "IInput":
        """
        Get the input with the given full name.

        If the input belongs to this cell, it should be just the ordinary input name.
        If the input belongs to an internal cell, the full name should be relative to this cell.

        Args:
            full_name: the full name of the input to get.
        Returns:
            The input of this cell, or one of the internal cells, with the given full name.
        Raises:
            KeyError: this cell and its internal cells have no input with the given name.
        """
        return self.get_input(full_name)

    def has_as_input(self, cell_input: "IInput") -> bool:
        """
        Args:
            cell_input: an input object.
        Returns:
            True if the given input is part of this cell, false otherwise.
        """
        return cell_input in self.__inputs.values()

    # ------ Outputs ------

    def get_outputs(self) -> Sequence["IOutput"]:
        """
        Returns:
            All outputs of this cell.
        """
        return tuple(self.__outputs.values())

    def get_output_names(self) -> Sequence[str]:
        """
        Returns:
            The names of all outputs of this cell.
        """
        return tuple(self.__outputs.keys())

    def get_output(self, name: str) -> "IOutput":
        """
        Raises:
            KeyError: this cell has no output with the given name.
        """
        return self.__outputs[name]

    def get_outputs_recursively(self) -> Sequence["IOutput"]:
        """
        Returns:
            All outputs of this cell, and those of its internal cells.
        """
        return self.get_outputs()

    def get_output_recursively(self, full_name: str) -> "IOutput":
        """
        Get the output with the given full name.

        If the output belongs to this cell, it should be just the ordinary output name.
        If the output belongs to an internal cell, the full name should be relative to this cell.

        Args:
            full_name: the full name of the output to get.
        Returns:
            The output of this cell, or one of the internal cells, with the given full name.
        Raises:
            KeyError: this cell and its internal cells have no output with the given name.
        """
        return self.get_output(full_name)

    def has_as_output(self, cell_output: "IOutput") -> bool:
        """
        Args:
            cell_output: an output object.
        Returns:
            True if the given output is part of this cell, false otherwise.
        """
        return cell_output in self.__outputs.values()

    # ------ Topology ------

    def is_source_cell(self) -> bool:
        """
        Source cells are cells that provide data to a pipeline.

        Source cells in a pipeline are the ones that need to be executed before any of the other cells.
        See also sink cells. A cell can be both a source and a sink at the same time.

        Returns:
            True if this cell is a source cell.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        parent_cell = self.get_parent_cell()
        if parent_cell is None:
            return True
        return parent_cell._has_as_internal_source_cell(self)   # access to protected method on purpose # TODO may raise exceptions

    def is_sink_cell(self) -> bool:
        """
        Sink cells are cells that remove data from a pipeline.

        Sink cells in a pipeline are the ones that can only be executed after all other cells.
        See also source cells. A cell can be both a source and a sink at the same time.

        Returns:
            True if this cell is a sink cell.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        parent_cell = self.get_parent_cell()
        if parent_cell is None:
            return True
        return parent_cell._has_as_internal_sink_cell(self)     # access to protected method on purpose # TODO may raise exceptions

    def get_topology_description(self) -> Dict[str, Any]:
        """
        Returns:
            A dictionary (json format), fully describing the topological properties if this cell.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        result = {
            "cell": str(self),
            "type": self.__class__.__name__,
            "name": self.get_name(),
            "id": self.get_full_name(),
            "is_source_cell": self.is_source_cell(),
            "is_sink_cell": self.is_sink_cell(),
            "inputs": [input_.get_topology_description() for input_ in self.get_inputs()],
            "outputs": [output.get_topology_description() for output in self.get_outputs()]
        }
        return result

    # ------ State synchronization between a cell and its clones ------

    def get_observers(self) -> Sequence["IObserver"]:
        """
        Returns:
            All observers of this cell.
        """
        return tuple(self.__observers)

    def _add_observer(self, observer: "IObserver") -> None:
        """
        Auxiliary mutator in the IObserver-IObservable relation, as observable.
        -> Should only be used by IObserver instances when registering this as observable.

        Args:
            observer: the observer to add.
        Raises:
            InvalidInputException
        """
        raise_if_not(self.can_have_as_observer(observer), InvalidInputException,
                     f"{self} cannot have {observer} as observer: ")
        if self.has_as_observer(observer):
            raise InvalidInputException(f"{self} already has {observer} as observer.")
        self.__observers.append(observer)

    def _remove_observer(self, observer: "IObserver") -> None:
        """
        Auxiliary mutator in the IObserver-IObservable relation, as observable.
        -> Should only be used by IObserver instances when unregistering this as observable.

        Args:
            observer: the observer to remove.
        Raises:
            InvalidInputException
        """
        if not self.has_as_observer(observer):
            raise InvalidInputException(f"{self} does not have {observer} as observer")
        self.__observers.remove(observer)

    @classmethod
    def can_have_as_observer(cls, observer: "IObserver") -> BoolExplained:
        """
        Args:
            observer: observer to validate
        Returns:
            TrueExplained if the given observer is valid. FalseExplained otherwise.
        """
        if not isinstance(observer, pypipeline.cell.icellobserver.IObserver):
            return FalseExplained(f"Observer should be of type IObserver, got {type(observer)}")
        return TrueExplained()

    def has_as_observer(self, observer: "IObserver") -> bool:
        """
        Args:
            observer: an observer object.
        Returns:
            True if the given observer is observing this cell, false otherwise.
        """
        return observer in self.__observers

    def assert_has_proper_observers(self) -> None:
        """
        Raises:
            InvalidStateException: if one of this cell's observers is invalid.
        """
        observers = self.get_observers()
        for observer in observers:
            raise_if_not(self.can_have_as_observer(observer), InvalidStateException, f"{self} has invalid observer: ")
            if not observer.has_as_observable(self):
                raise InvalidStateException(f"Inconsistent relation: {observer} doesn't have {self} as observable.")
            if observers.count(observer) > 1:
                raise InvalidStateException(f"{self} has {observer} multiple times as observer.")

    def notify_observers(self, event: "Event") -> None:
        """
        Notify all observers of this cell that the given event just happened.

        Args:
            event: an event object, indicating what the observers should be notified of.
        Raises:
            TODO may raise exceptions?
        """
        for observer in self.get_observers():
            if self.is_deployed():
                self.logger.debug(f"{self} notifying observer {observer} of internal state change")
            observer.update(event)

    def _get_sync_state(self) -> Dict[str, Any]:
        """
        Used for synchronizing the state of clone cells with that of their corresponding original one.

        Returns:
            A (nested) dictionary, containing the state of this cell.
        """
        with self._get_pull_lock():
            state: Dict[str, Any] = dict()

            # input states
            input_states = {input_.get_name(): input_._get_sync_state() for input_ in self.get_inputs()}    # access to protected method on purpose
            state[self.__INPUT_STATES_KEY] = input_states
        return state

    def _set_sync_state(self, state: Dict) -> None:
        """
        Used for synchronizing the state of clone cells with that of their corresponding original one.

        Args:
            state: a (nested) dictionary, containing the new state of this cell.
        Raises:
            KeyError: when the state dictionary contains the name of a cell or IO that is not available in this cell.
        """
        self.logger.debug(f"{self} setting sync state: \n{pformat(state)}")
        assert self.__INPUT_STATES_KEY in state

        with self._get_pull_lock():
            # input states
            for input_name, input_state in state[self.__INPUT_STATES_KEY].items():
                input_ = self.get_input(input_name)
                input_._set_sync_state(input_state)     # access to protected method on purpose

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "ACell":
        """
        Clone this cell to a new parent cell.

        Override this method if your cell instantiation requires more/other arguments than implemented here.
        If the cell doesn't support being cloned, raise NotImplementedError.

        Args:
            new_parent: the parent to which this cell should be cloned.
        Returns:
            The cloned cell.
        Raises:
            NotImplementedError: if this cell doesn't support cloning.
        """
        cell_type: Type["ACell"] = type(self)
        return cell_type(new_parent, self.get_name())       # TODO raise proper exception when the cell has another __init__ signature.

    def supports_scaling(self) -> bool:
        """
        A cell support scaling if it supports multiple clones of it running in parallel. This should usually be False
        for stateful cells.

        Returns:
            True if this cell supports scaling. False otherwise.
        """
        raise NotImplementedError

    # ------ Deployment ------

    def inputs_are_provided(self) -> "BoolExplained":
        """
        Returns:
            TrueExplained if all inputs of this cell are provided (they have an incoming connection, a default,
            value, ...)
        """
        result: BoolExplained = TrueExplained()
        for input_ in self.get_inputs():
            if not input_.is_provided():
                self.logger.warning(f"Johannes: {input_} is not provided!")
                result *= FalseExplained(f"{input_} is not provided. Please set a value or make a connection to it.")
        return result

    def is_deployable(self) -> "BoolExplained":
        """
        Returns:
            TrueExplained if all preconditions for deployment are met. FalseExplained otherwise.
        """
        return self.inputs_are_provided()

    def deploy(self) -> None:
        """
        Deploy this cell.

        Deploying prepares a cell to be executed. This includes (not exhaustively):
         - validating whether all preconditions for deployment are met
         - acquiring resources needed to run the cell
         - spawning cell clones

        Raises:
            AlreadyDeployedException: if this cell (or an internal cell) is already deployed.
            NotDeployableException: if the cell cannot be deployed because some preconditions are not met.
            Exception: any exception that the user may raise when overriding _on_deploy or _on_undeploy.
        """
        with self._get_pull_lock():
            if self.is_deployed():
                raise AlreadyDeployedException(f"{self} is already deployed.")
            raise_if_not(self.is_deployable(), NotDeployableException, f"Cannot deploy {self}: ")
            self.logger.info(f"Deploying {self}...")
            self._on_deploy()
            self.__is_deployed = True

    def _on_deploy(self) -> None:
        """
        Override this method to add functionality that must happen when deploying the cell.

        Don't forget to call the _on_deploy of the super-class when overriding this method! Ex:
        ```
        def _on_deploy(self) -> None:
            super(MyCell, self)._on_deploy()
            # other deployment code
        ```

        Raises:
            AlreadyDeployedException: if called when an internal cell is already deployed.
            IndeterminableTopologyException: if the internal topology could not be determined.
                Cannot happen in deploy(), as the is_deployable() method covers this case already.
                Exception: any exception that the user may raise when overriding _on_deploy or _on_undeploy.
            """
        for io in self.get_all_io():
            io._deploy()        # access to protected member on purpose

    def undeploy(self) -> None:
        """
        Undeploy this cell.

        Opposite of deploying. This includes (not exhaustively):
         - releasing resources that the cell acquired
         - removing all spawned cell clones

        Raises:
            NotDeployedException: if this cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_undeploy.
        """
        with self._get_pull_lock():
            if not self.is_deployed():
                raise NotDeployedException(f"{self} is not deployed.")
            self.logger.info(f"Undeploying {self}...")
            self._on_undeploy()
            self.__is_deployed = False

    def _on_undeploy(self) -> None:
        """
        Override this method to add functionality that must happen when undeploying the cell.

        Don't forget to call the _on_undeploy of the super-class when overriding this method! Ex:
        ```
        def _on_undeploy(self) -> None:
            super(MyCell, self)._on_undeploy()
            # other undeployment code
        ```

        Raises:
            NotDeployedException: if called when an internal cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_undeploy.
        """
        for io in self.get_all_io():
            io._undeploy()        # access to protected member on purpose

    def is_deployed(self) -> bool:
        """
        Returns:
            True if this cell is deployed, False otherwise.
        """
        with self._get_pull_lock():
            return self.__is_deployed

    def assert_is_properly_deployed(self) -> None:
        """
        Raises:
            InvalidStateException: if the cell is not properly deployed.
        """
        with self._get_pull_lock():
            for io in self.get_all_io():
                io_is_deployed = io._is_deployed()      # access to protected member on purpose
                if io_is_deployed != self.is_deployed():
                    raise InvalidStateException(f"{self}.is_deployed() == {self.is_deployed()}, but "
                                                f"{io}._is_deployed() == {io_is_deployed}")
                io._assert_is_properly_deployed()

    # ------ Pulling ------

    def pull_as_output(self, output: "IOutput") -> None:        # TODO hide in public interface -> make protected?
        """
        Cells should only execute again when all their outputs have pulled the previous result.
        This is to avoid that the cell is executed again while another downstream cell still needed an old output
        value.
        For example: imagine two ScalableCells each connected to a different output of a source cell. If the first
        scalablecell executes faster than the second one, it might request to pull the source cell multiple times while
        the other has only pulled the source cell once. If this pull would not block, the other scalablecell would miss
        some source outputs.

        Note: calling this method will block forever when an output pulls twice while all other outputs don't pull at
        all.

        Args:
            output: the output that wants to pull this cell.
        Raises:
            NotDeployedException: if the cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_pull.
            TODO may raise exceptions (more than written here, same as self.pull() ?)
        """
        self.logger.debug(f"{self}.pull_as_output()")

        # Full method is a critical section
        with self.__pull_as_output_lock:
            self.logger.debug(f"{self} flag 1")

            assert self.has_as_output(output)

            # If 2 outputs pull a cell at the same time, the first one will get the lock and perform cell
            # execution. Afterwards, the second output will enter the pull lock. This should not lead to new
            # cell execution AND IT SHOULD NOT HAVE TO WAIT UNTIL OTHER EVENTUAL OUTPUTS HAVE BEEN FULLY PULLED.

            # Check whether the cell ever has been pulled already.
            # If not, immediately skip to the end of this method. Otherwise:
            while self.__has_been_pulled_at_least_once:
                self.logger.debug(f"{self} flag 2")

                # Check whether the output has not yet been set in the meantime (ex: after having waited for pull_lock)
                if not output.all_outgoing_connections_have_pulled():
                    self.logger.debug(f"{self} flag 3")
                    # the output has been set
                    self.__pull_as_output_lock.notify_all()
                    return

                # Check if any other output still needs to see its current output value before a new one
                # is set. If so, wait until another connection has pulled, and then let this connection try pulling
                # again.
                elif not self.all_outputs_have_been_pulled():
                    self.logger.debug(f"{self} flag 4")
                    # Wait before trying to pull again (continue).
                    while not self.__pull_as_output_lock.wait(timeout=self.PULL_TIMEOUT):
                        if self.all_outputs_have_been_pulled():
                            # Can happen when one of the outgoing connections got undeployed.
                            self.logger.debug(f"{self} flag 4.1")
                            break
                    self.logger.debug(f"{self} flag 4.2")
                    continue

                # All outputs, including this one, have been fully pulled, so it's time to pull the cell.
                else:
                    self.logger.debug(f"{self} flag 5")
                    break

            self.logger.debug(f"{self} flag 6")

            # Pull a new value
            self.pull()
            self.__pull_as_output_lock.notify_all()

            self.logger.debug(f"{self}.pull_as_output() done")

    def pull(self) -> None:
        """
        During a pull, a cell will pull its inputs, execute it's functionality and set its outputs.

        Since a cell pulls its inputs before it executes, all prerequisite cells are pulled recursively up to
        the source cells, before this pull finishes.

        Raises:
            NotDeployedException: if the cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_pull.
            # TODO may raise exceptions (more exceptions in case of scalable cells etc...)

        Won't raise:
            IndeterminableTopologyException: this method can only be called when the cell is already deployed.
        """
        with self._get_pull_lock():
            if not self.is_deployed():
                raise NotDeployedException(f"You must deploy first before you can pull: {self}.deploy()")
            self._on_pull()
            self.__has_been_pulled_at_least_once = True

    def _on_pull(self) -> None:
        """
        Override this method to add functionality that must happen when pulling the cell.

        During a pull, a cell must pull its inputs, execute it's functionality and set its outputs.

        Raises:
            Exception: any exception that the user may raise when overriding _on_pull.

       Won't raise:
            NotDeployedException: this method will only be called when the cell is already deployed.
            IndeterminableTopologyException: this method will only be called when the cell is already deployed.
        """
        raise NotImplementedError

    def all_outputs_have_been_pulled(self) -> bool:
        """
        Returns:
            True if all the outputs of this cell have been pulled if needed, meaning that they are ready to receive new
            output values (a new cell.pull() can be done).
        """
        with self.__pull_as_output_lock:
            all_have_been_pulled = True
            for output in self.get_outputs():
                if not output.all_outgoing_connections_have_pulled():
                    all_have_been_pulled = False
                    break
        return all_have_been_pulled

    def _notify_connection_has_pulled(self) -> None:
        """
        Notifies the cell that one of the outgoing connections successfully pulled, and that other threads waiting
        for the pull lock may be notified.
        """
        with self.__pull_as_output_lock:
            self.__pull_as_output_lock.notify_all()

    def get_nb_available_pulls(self) -> Optional[int]:
        """
        Returns the total number of times this cell can be pulled.

        Default implementation here is only valid if the cell pulls all its inputs once per cell pull. If this is
        not the case, the cell must override this method. Also when a cell has no inputs, it has to override this
        method itself.

        Returns:
            The total number of times this cell can be pulled, or
            None if cell can be pulled infinitely or an unspecified amount of time.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        if self.is_source_cell():
            raise NotImplementedError(f"{self}: a source cell should override get_nb_available_pulls()")
        inputs = self.get_inputs()
        available_pulls_per_input = [input_.get_nb_available_pulls() for input_ in inputs]
        available_pulls_per_input_no_none = [nb for nb in available_pulls_per_input if nb is not None]
        if len(available_pulls_per_input_no_none) == 0:
            return None
        return min(available_pulls_per_input_no_none)

    def reset(self) -> None:
        """
        Reset the internal state of a cell and its prerequisite cells.

        Just like the pull() method, this reset is recursively propagated through all prerequisite cells
        up to the source cells.

        Resetting internal state does not include cell configuration, but only cell state that was accumulated
        during consecutive pulls.
        Ex: reset the dataloader iterator of a dataloader cell.
        Ex: the currently accumulated batch in a BatchingCell.

        Raises:
            NotDeployedException: if the cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_reset.
            # TODO may raise exceptions (more exceptions in case of scalable cells etc...)
        """
        with self._get_pull_lock():
            self.logger.info(f"{self}.reset()")
            if not self.is_deployed():
                raise NotDeployedException(f"{self}: a cell cannot be reset when it's not deployed.")
            if self.__reset_is_busy:
                # recurrently called
                return
            self.__reset_is_busy = True
            self._on_reset()
            self.__reset_is_busy = False

    def _on_reset(self) -> None:
        """
        Override this method to add functionality that must happen when resetting the cell.

        During a reset, a cell must clear its internal state. This doesn't include cell configuration, but only
        cell state that was accumulated during consecutive pulls.
        Ex: reset the dataloader iterator of a dataloader cell.
        Ex: the currently accumulated batch in a BatchingCell.

        Don't forget to call the _on_reset of the super-class when overriding this method! Ex:
        ```
        def _on_reset(self) -> None:
            super(MyCell, self)._on_reset()
            # other resetting code
        ```

        Raises:
            Exception: any exception that the user may raise when overriding _on_reset.
        """
        for input_ in self.get_inputs():
            input_.reset()
        self.__has_been_pulled_at_least_once = False

    def get_nb_required_gpus(self) -> float:
        """
        Override this method to indicate how much GPUs your cell needs.

        Note: very experimental feature - might not yet work at all.

        Returns:
            The number of GPUs this cell needs. Can be a fraction.
        """
        raise NotImplementedError

    # ------ General validation ------

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if one of the attributes or relations of this cell is invalid.
        """
        self.assert_has_proper_name()
        self.assert_has_proper_parent_cell()
        self.assert_has_proper_io()
        self.assert_has_proper_observers()
        self.assert_is_properly_deployed()

    # ------ Deletion ------

    def can_be_deleted(self) -> BoolExplained:
        """
        Do not override this method... Or if you do, make sure it can succeed even when the object is already deleted.
        """
        if self.is_deployed():
            raise AlreadyDeployedException(f"{self} cannot be deleted while it is deployed.")
        for input_ in self.get_inputs():
            if input_.get_nb_incoming_connections() > 0:
                return FalseExplained(f"Cannot delete {self}, as it still has "
                                      f"{input_.get_nb_incoming_connections()} incoming connections.")
        for output in self.get_outputs():
            if output.get_nb_outgoing_connections() > 0:
                return FalseExplained(f"Cannot delete {self}, as it still has "
                                      f"{output.get_nb_outgoing_connections()} outgoing connections.")
        return TrueExplained()

    def delete(self) -> None:
        """
        Deletes this cell, and all its internals.

        Raises:
            CannotBeDeletedException
        """
        raise_if_not(self.can_be_deleted(), CannotBeDeletedException)
        if self.__parent_cell is not None:
            self.__parent_cell._remove_internal_cell(self)      # Access to protected method on purpose
        for io in self.get_all_io():
            io.delete()
        assert len(self.get_observers()) == 0, f"{self} is deleted, but has observers left: {self.get_observers()}"

    # ------ Magic methods ------

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.get_full_name()})"

    def __getstate__(self) -> Dict:
        # called during pickling (transfer of this object to other memory space, ex: ray clone)
        new_state = dict(self.__dict__)
        new_state["_ACell__pull_as_output_lock"] = None     # Locks cannot be pickled
        new_state["_ACell__pull_lock"] = None               # Locks cannot be pickled
        # self.logger.debug(f"{self}.__getstate__() : \n{pformat(new_state)}")
        return new_state

    def __setstate__(self, state: Dict) -> None:
        # called during unpickling (transfer of this object from other memory space, ex: ray clone)
        self.__dict__ = state
        self.__pull_as_output_lock = Condition(RLock())
        self.__pull_lock = RLock()
        # self.logger.debug(f"{self}.__setstate__() : \n{pformat(state)}")
