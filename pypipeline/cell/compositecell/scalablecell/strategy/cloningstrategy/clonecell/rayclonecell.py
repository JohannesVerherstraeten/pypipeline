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

from typing import TypeVar, List, Any, Optional, TYPE_CHECKING, Dict, Union
import ray
import logging
from pprint import pformat

from pypipeline.connection import Connection
from pypipeline.cellio import IConnectionEntryPoint, IConnectionExitPoint, InternalOutput, InternalInput
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ACloneCell
from pypipeline.cell.compositecell.pipeline import Pipeline
from pypipeline.cell.icellobserver import Event, CloneCreatedEvent
from pypipeline.exceptions import InvalidStateException, CannotBeDeletedException
from pypipeline.validation import raise_if_not

if TYPE_CHECKING:
    from pypipeline.cell.icell import ICell
    from pypipeline.cell.compositecell.icompositecell import ICompositeCell


T = TypeVar('T')


@ray.remote
class RayActorPipeline(Pipeline):
    """
    Ray actor pipeline class.

    This pipeline runs in a separate process (or Ray actor). Therefore, when calling this class's methods,
    always use `.remote()`.

    Inside the Ray actor process, this class sets up a pipeline environment for the internal cell to run in.
    """

    def __init__(self,
                 internal_cell: "ICell",
                 name: str,
                 logging_level: int = logging.WARNING,
                 logging_format: Optional[str] = None,
                 logging_datefmt: Optional[str] = None):
        logging_params = {}
        if logging_format is not None:
            logging_params["format"] = logging_format
        logging_params["datefmt"] = logging_datefmt
        logging.basicConfig(level=logging_level, **logging_params)
        super().__init__(None, name, max_nb_internal_cells=1)

        self.logger.info(f"  RayActor.__init__:  creating remote pipeline {self}...")

        internal_cell = internal_cell.clone(self)       # add self as parent cell   # TODO may raise exceptions

        for internal_cell_input in internal_cell.get_inputs():
            if isinstance(internal_cell_input, IConnectionEntryPoint):
                ray_actor_input: InternalInput[Any] = InternalInput(self, internal_cell_input.get_name())
                Connection(ray_actor_input, internal_cell_input)

        for i, internal_cell_output in enumerate(internal_cell.get_outputs()):
            if isinstance(internal_cell_output, IConnectionExitPoint):
                ray_actor_output: InternalOutput[Any] = InternalOutput(self, internal_cell_output.get_name())
                Connection(internal_cell_output, ray_actor_output)

        self.logger.info(f"  RayActor.__init__:  creating remote pipeline {self} done")

    def get_internal_cell(self) -> "ICell":
        internal_cell_list = self.get_internal_cells()
        assert len(internal_cell_list) == 1
        return internal_cell_list[0]

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "RayActorPipeline":
        raise Exception()   # should never be called

    def run(self, input_elements: Dict[str, Any]) -> List[Any]:
        for input_ in self.get_inputs():
            assert isinstance(input_, InternalInput)
            assert input_.get_name() in input_elements
            input_element = input_elements[input_.get_name()]
            input_.set_value(input_element)
        result: List[Any]
        self.get_internal_cell().pull()
        result = [output.pull() for output in self.get_outputs()]
        return result

    def set_internal_cell_sync_state(self, new_state: Dict[str, Any]) -> None:
        self.logger.debug(f"Setting sync state of ray actor: ")
        self.logger.debug("\n" + pformat(new_state))
        self.get_internal_cell()._set_sync_state(new_state)     # Access to protected   # TODO may raise exceptions


class RayCloneCell(ACloneCell):
    """
    Ray clone cell class.

    This class abstracts away all Ray-related stuff: a ScalableCell can use it just like any other clone cell.
    Behind the scenes, this clone cell creates the a clone of the original cell in a separate Ray actor process.
    """

    def __init__(self, original_cell: "ICell", name: str):
        # A Ray actor pipeline is not considered as internal cell, as it runs in a different process.
        super(RayCloneCell, self).__init__(original_cell, name, max_nb_internal_cells=0)

        required_gpus = original_cell.get_nb_required_gpus()

        # A clone of the cell _with_any_reference_to_its_parent_cells_removed_ must be transferred to the ray process.
        # Because, as the parent cell is a ScalableCell, it contains threadsafety objects like Locks, Conditions, etc..
        # which cannot be pickled for transfer.
        original_cell_clone = original_cell.clone(None)     # TODO may raise exceptions

        logger_format = logging.root.handlers[0].formatter._fmt if len(logging.root.handlers) > 0 else None
        logger_datefmt = logging.root.handlers[0].formatter.datefmt if len(logging.root.handlers) > 0 else None
        self.actor_ref: RayActorPipeline = RayActorPipeline.options(num_gpus=required_gpus)\
                                                           .remote(original_cell_clone,
                                                                   name,
                                                                   logging.root.level,
                                                                   logger_format,
                                                                   logger_datefmt)

        self.update(CloneCreatedEvent(self))
        self.logger.info(f"  RayActor created: {self.actor_ref}")

    @classmethod
    def create(cls, original_cell: "ICell", name: str) -> "RayCloneCell":
        """
        Factory method to create a new clone.

        Args:
            original_cell: the original cell to be cloned.
            name: the name of the new clone cell.
        Returns:
            A new clone cell.
        Raises:
            InvalidInputException
            NotImplementedError: if the original cell doesn't support cloning.
        """
        return RayCloneCell(original_cell, name)

    def _on_pull(self) -> None:
        """
        Override this method to add functionality that must happen when pulling the cell.

        During a pull, a cell must pull its inputs, execute it's functionality and set its outputs.

        Raises:
            StopIteration: when the end of the pipeline is reached.
            Exception: any exception that the user may raise when overriding _on_pull.

        Won't raise:
            NotDeployedException: this method will only be called when the cell is already deployed.
            IndeterminableTopologyException: this method will only be called when the cell is already deployed.
        """
        # self.logger.debug(f"{self}.pull()")
        input_elements: Dict[str, Union[Any, Exception]] = {}
        for input_ in self.get_clone_inputs():
            input_value: Union[Any, Exception] = input_.pull()
            input_elements[input_.get_name()] = input_value
        # self.logger.debug(f"{self} executing remote run()...")
        results_id = self.actor_ref.run.remote(input_elements)     # type: ignore
        results: List[Any] = ray.get(results_id)       # If the remote run() causes exceptions, they are raised here.
        # self.logger.debug(f"{self} remote run ready: inputs {input_elements} and setting outputs to {results}")
        assert len(results) == len(self.get_clone_outputs())
        for output, result in zip(self.get_clone_outputs(), results):
            output.set_value(result)

    def update(self, event: "Event") -> None:
        """
        Will be called by the observables of this observer (ex: internal cell).

        What should trigger this update:
        - a parameter of the observed cell changes,

        Args:
            event: the event to notify this observer about.
        """
        super(RayCloneCell, self).update(event)
        sync_state = self.get_original_cell()._get_sync_state()
        self.actor_ref.set_internal_cell_sync_state.remote(sync_state)   # type: ignore

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
        super(RayCloneCell, self)._on_deploy()
        self.actor_ref.deploy.remote()          # type: ignore  # TODO may raise exceptions

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
        super(RayCloneCell, self)._on_undeploy()
        self.actor_ref.undeploy.remote()        # type: ignore  # TODO may raise exceptions

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
        super(RayCloneCell, self)._on_reset()
        self.actor_ref.reset.remote()     # type: ignore

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException
        """
        super(RayCloneCell, self).assert_is_valid()
        raise_if_not(self.actor_ref.assert_is_valid.remote(), InvalidStateException)    # type: ignore

    def delete(self) -> None:
        """
        Deletes this cell, and all its internals.

        Raises:
            CannotBeDeletedException
        """
        raise_if_not(self.can_be_deleted(), CannotBeDeletedException)
        self.actor_ref.delete.remote()              # type: ignore  # TODO may raise exceptions
        self.actor_ref = None
        super(RayCloneCell, self).delete()
