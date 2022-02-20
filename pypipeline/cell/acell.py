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
    PULL_TIMEOUT: float = 5.0       # TODO use configparameter, like ScalableCell.config_check_quit_interval

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
        return self.__name

    def get_full_name(self) -> str:
        parent_cell = self.get_parent_cell()
        prefix = parent_cell.get_full_name() + "." if parent_cell is not None else ""
        return prefix + self.get_name()

    @classmethod
    def can_have_as_name(cls, name: str) -> BoolExplained:
        if not isinstance(name, str):
            return FalseExplained(f"Name should be a string, got {name}")
        elif len(name) == 0:
            return FalseExplained(f"Name should not be an empty string")
        elif "." in name:
            return FalseExplained(f"Name should not contain `.`")
        return TrueExplained()

    def assert_has_proper_name(self) -> None:
        raise_if_not(self.can_have_as_name(self.get_name()), InvalidStateException, f"{self} has invalid name: ")

    # ------ Parent cell ------

    def get_parent_cell(self) -> "Optional[ICompositeCell]":
        return self.__parent_cell

    def _set_parent_cell(self, parent_cell: "Optional[ICompositeCell]") -> None:
        raise_if_not(self.can_have_as_parent_cell(parent_cell), InvalidInputException)
        if self.get_parent_cell() is not None and parent_cell is not None:
            raise InvalidInputException(f"{self} already has a parent cell: {self.get_parent_cell()}")
        self.__parent_cell = parent_cell

    @classmethod
    def can_have_as_parent_cell(cls, cell: "Optional[ICompositeCell]") -> BoolExplained:
        if cell is None:
            return TrueExplained()
        if not isinstance(cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell):
            return FalseExplained(f"A cell's parent cell must be a subtype of ICompositeCell or None, "
                                  f"got {type(cell)}")
        return TrueExplained()

    def assert_has_proper_parent_cell(self) -> None:
        parent_cell = self.get_parent_cell()
        raise_if_not(self.can_have_as_parent_cell(parent_cell), InvalidStateException,
                     f"{self} has an invalid parent cell: ")
        if parent_cell is not None and not parent_cell.has_as_internal_cell(self):
            raise InvalidStateException(f"Inconsistent relation: {parent_cell} doesn't have {self} as internal cell. ")

    # ------ General IO ------

    def get_all_io(self) -> Sequence["IO"]:
        result: List["IO"] = list(self.__inputs.values())
        result.extend(self.__outputs.values())
        return result

    def get_io(self, name: str) -> "IO":
        if name in self.__inputs:
            return self.__inputs[name]
        elif name in self.__outputs:
            return self.__outputs[name]
        else:
            raise KeyError(f"Cell {self} has no input or output with name {name}")

    def get_io_names(self) -> Sequence[str]:
        return [io.get_name() for io in self.get_all_io()]

    def has_as_io(self, io: "IO") -> bool:
        return io in self.__inputs.values() or io in self.__outputs.values()

    def can_have_as_io(self, io: "IO") -> "BoolExplained":
        if not isinstance(io, pypipeline.cellio.icellio.IO):
            return FalseExplained(f"Cell IO must be of type IO, got {type(io)}")
        if not isinstance(io, (pypipeline.cellio.IInput, pypipeline.cellio.IOutput)):
            return FalseExplained(f"Cell IO must be either a subtype of IInput or IOutput, got {type(io)}")
        return TrueExplained()

    def _add_io(self, io: "IO") -> None:
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
        for io in self.get_all_io():
            raise_if_not(self.can_have_as_io(io), InvalidStateException)
            if io.get_cell() != self:
                raise InvalidStateException(f"Inconsistent relation: {io} doesn't have {self} as cell.")
            io.assert_is_valid()

    # ------ Inputs ------

    def get_inputs(self) -> Sequence["IInput"]:
        return tuple(self.__inputs.values())

    def get_input_names(self) -> Sequence[str]:
        return tuple(self.__inputs.keys())

    def get_input(self, name: str) -> "IInput":
        return self.__inputs[name]

    def get_inputs_recursively(self) -> Sequence["IInput"]:
        return self.get_inputs()

    def get_input_recursively(self, full_name: str) -> "IInput":
        return self.get_input(full_name)

    def has_as_input(self, cell_input: "IInput") -> bool:
        return cell_input in self.__inputs.values()

    # ------ Outputs ------

    def get_outputs(self) -> Sequence["IOutput"]:
        return tuple(self.__outputs.values())

    def get_output_names(self) -> Sequence[str]:
        return tuple(self.__outputs.keys())

    def get_output(self, name: str) -> "IOutput":
        return self.__outputs[name]

    def get_outputs_recursively(self) -> Sequence["IOutput"]:
        return self.get_outputs()

    def get_output_recursively(self, full_name: str) -> "IOutput":
        return self.get_output(full_name)

    def has_as_output(self, cell_output: "IOutput") -> bool:
        return cell_output in self.__outputs.values()

    # ------ Topology ------

    def is_source_cell(self) -> bool:
        parent_cell = self.get_parent_cell()
        if parent_cell is None:
            return True
        return parent_cell._has_as_internal_source_cell(self)   # access to protected method on purpose # TODO may raise exceptions

    def is_sink_cell(self) -> bool:
        parent_cell = self.get_parent_cell()
        if parent_cell is None:
            return True
        return parent_cell._has_as_internal_sink_cell(self)     # access to protected method on purpose # TODO may raise exceptions

    def get_topology_description(self) -> Dict[str, Any]:
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
        return tuple(self.__observers)

    def _add_observer(self, observer: "IObserver") -> None:
        raise_if_not(self.can_have_as_observer(observer), InvalidInputException,
                     f"{self} cannot have {observer} as observer: ")
        if self.has_as_observer(observer):
            raise InvalidInputException(f"{self} already has {observer} as observer.")
        self.__observers.append(observer)

    def _remove_observer(self, observer: "IObserver") -> None:
        if not self.has_as_observer(observer):
            raise InvalidInputException(f"{self} does not have {observer} as observer")
        self.__observers.remove(observer)

    @classmethod
    def can_have_as_observer(cls, observer: "IObserver") -> BoolExplained:
        if not isinstance(observer, pypipeline.cell.icellobserver.IObserver):
            return FalseExplained(f"Observer should be of type IObserver, got {type(observer)}")
        return TrueExplained()

    def has_as_observer(self, observer: "IObserver") -> bool:
        return observer in self.__observers

    def assert_has_proper_observers(self) -> None:
        observers = self.get_observers()
        for observer in observers:
            raise_if_not(self.can_have_as_observer(observer), InvalidStateException, f"{self} has invalid observer: ")
            if not observer.has_as_observable(self):
                raise InvalidStateException(f"Inconsistent relation: {observer} doesn't have {self} as observable.")
            if observers.count(observer) > 1:
                raise InvalidStateException(f"{self} has {observer} multiple times as observer.")

    def notify_observers(self, event: "Event") -> None:
        for observer in self.get_observers():
            if self.is_deployed():
                self.logger.debug(f"{self} notifying observer {observer} of internal state change")
            observer.update(event)

    def _get_sync_state(self) -> Dict[str, Any]:
        with self._get_pull_lock():
            state: Dict[str, Any] = dict()

            # input states
            input_states = {input_.get_name(): input_._get_sync_state() for input_ in self.get_inputs()}    # access to protected method on purpose
            state[self.__INPUT_STATES_KEY] = input_states
        return state

    def _set_sync_state(self, state: Dict) -> None:
        self.logger.debug(f"{self} setting sync state: \n{pformat(state)}")
        assert self.__INPUT_STATES_KEY in state

        with self._get_pull_lock():
            # input states
            for input_name, input_state in state[self.__INPUT_STATES_KEY].items():
                input_ = self.get_input(input_name)
                input_._set_sync_state(input_state)     # access to protected method on purpose

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "ACell":
        cell_type: Type["ACell"] = type(self)
        return cell_type(new_parent, self.get_name())       # TODO raise proper exception when the cell has another __init__ signature.

    def supports_scaling(self) -> bool:
        raise NotImplementedError

    # ------ Deployment ------

    def inputs_are_provided(self) -> "BoolExplained":
        result: BoolExplained = TrueExplained()
        for input_ in self.get_inputs():
            if not input_.is_provided():
                self.logger.warning(f"Johannes: {input_} is not provided!")
                result *= FalseExplained(f"{input_} is not provided. Please set a value or make a connection to it.")
        return result

    def is_deployable(self) -> "BoolExplained":
        return self.inputs_are_provided()

    def deploy(self) -> None:
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
            NotDeployableException: if this cell cannot be deployed.
            AlreadyDeployedException: if called when an internal cell is already deployed.
            IndeterminableTopologyException: if the internal topology could not be determined.
                Cannot happen in deploy(), as the is_deployable() method covers this case already.
                Exception: any exception that the user may raise when overriding _on_deploy or _on_undeploy.
            """
        for io in self.get_all_io():
            io._deploy()        # access to protected member on purpose

    def undeploy(self) -> None:
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
        with self._get_pull_lock():
            return self.__is_deployed

    def assert_is_properly_deployed(self) -> None:
        with self._get_pull_lock():
            for io in self.get_all_io():
                io_is_deployed = io._is_deployed()      # access to protected member on purpose
                if io_is_deployed != self.is_deployed():
                    raise InvalidStateException(f"{self}.is_deployed() == {self.is_deployed()}, but "
                                                f"{io}._is_deployed() == {io_is_deployed}")
                io._assert_is_properly_deployed()

    # ------ Pulling ------

    def pull_as_output(self, output: "IOutput") -> None:        # TODO hide in public interface -> make protected?
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
        with self._get_pull_lock():
            if not self.is_deployed():
                raise NotDeployedException(f"You must deploy first before you can pull: {self}.deploy()")
            self._on_pull()
            self.__has_been_pulled_at_least_once = True

    def _on_pull(self) -> None:
        """
        Override this method to add functionality that must happen when pulling the cell.

        During a pull, a cell must pull its inputs, execute its functionality and set its outputs.

        Raises:
            InactiveException: when a scalable cell is inactive (ex: it has no active scale-up worker threads).
            Exception: any exception that the user may raise when overriding _on_pull.

       Won't raise:
            NotDeployedException: this method will only be called when the cell is already deployed.
            IndeterminableTopologyException: this method will only be called when the cell is already deployed.
        """
        raise NotImplementedError

    def all_outputs_have_been_pulled(self) -> bool:
        with self.__pull_as_output_lock:
            all_have_been_pulled = True
            for output in self.get_outputs():
                if not output.all_outgoing_connections_have_pulled():
                    all_have_been_pulled = False
                    break
        return all_have_been_pulled

    def _notify_connection_has_pulled(self) -> None:
        with self.__pull_as_output_lock:
            self.__pull_as_output_lock.notify_all()

    def get_nb_available_pulls(self) -> Optional[int]:
        if self.is_source_cell():
            raise NotImplementedError(f"{self}: a source cell should override get_nb_available_pulls()")
        inputs = self.get_inputs()
        available_pulls_per_input = [input_.get_nb_available_pulls() for input_ in inputs]
        available_pulls_per_input_no_none = [nb for nb in available_pulls_per_input if nb is not None]
        if len(available_pulls_per_input_no_none) == 0:
            return None
        return min(available_pulls_per_input_no_none)

    def reset(self) -> None:
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
        raise NotImplementedError

    # ------ General validation ------

    def assert_is_valid(self) -> None:
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
