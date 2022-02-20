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
        """
        Args:
            internal_cell: the scalable cell's internal cell to wrap in an ray actor pipeline.
            name: the name of the new ray actor pipeline.
            logging_level: the logging level to use inside the ray actor process.
            logging_format: the logging format to use inside the ray actor process.
            logging_datefmt: the logging date format to use inside the ray actor process.
        """
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
    Behind the scenes, this clone cell creates a clone of the original cell in a separate Ray actor process.
    """

    def __init__(self, original_cell: "ICell", name: str):
        """
        Args:
            original_cell: the scalable cell's internal cell to create a clone from.
            name: the name of this ray clone cell.
        """
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
        return RayCloneCell(original_cell, name)

    def _on_pull(self) -> None:
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
        super(RayCloneCell, self).update(event)
        sync_state = self.get_original_cell()._get_sync_state()
        self.actor_ref.set_internal_cell_sync_state.remote(sync_state)   # type: ignore

    def _on_deploy(self) -> None:
        super(RayCloneCell, self)._on_deploy()
        self.actor_ref.deploy.remote()          # type: ignore  # TODO may raise exceptions

    def _on_undeploy(self) -> None:
        super(RayCloneCell, self)._on_undeploy()
        self.actor_ref.undeploy.remote()        # type: ignore  # TODO may raise exceptions

    def _on_reset(self) -> None:
        super(RayCloneCell, self)._on_reset()
        self.actor_ref.reset.remote()     # type: ignore

    def assert_is_valid(self) -> None:
        super(RayCloneCell, self).assert_is_valid()
        raise_if_not(self.actor_ref.assert_is_valid.remote(), InvalidStateException)    # type: ignore

    def delete(self) -> None:
        raise_if_not(self.can_be_deleted(), CannotBeDeletedException)
        self.actor_ref.delete.remote()              # type: ignore  # TODO may raise exceptions
        self.actor_ref = None
        super(RayCloneCell, self).delete()
