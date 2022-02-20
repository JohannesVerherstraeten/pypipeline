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

from typing import Optional, Dict, TYPE_CHECKING, Any, Type
import logging

import pypipeline
from pypipeline.cell.compositecell.scalablecell.circularmultiqueue import CircularMultiQueue
from pypipeline.cell.compositecell.scalablecell.strategy import AScalingStrategy, CloningStrategy
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ICloneCell, ThreadCloneCell
from pypipeline.cellio import OutputPort
from pypipeline.exceptions import InvalidStateException, TimeoutException, InactiveException, NotDeployedException, \
    NotDeployableException, InvalidInputException
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not

if TYPE_CHECKING:
    from pypipeline.cell.icell import ICell
    from pypipeline.cell.compositecell.scalablecell import ScalableCell


class ScalableCellDeployment:
    """
    Created when a scalable cell gets deployed.

    It holds all state that a scalable cell needs when deployed (the output queue, the pull strategy).
    -> Created to avoid a lot of conditional state in the ScalableCell class: "this state only exists if the scalable
       cell is deployed".

    A ScalableCellDeployment is the controlling class in the ScalableCellDeployment-ScalableCell relation,
    as the deployment of the scalable cell.
    -> Main mutators can be found in __init__ and delete()

    A ScalableCellDeployment is the controlled class in the AScalingStrategy-ScalableCellDeployment relation,
    as the owner of the scaling strategy.
    """

    def __init__(self, scalable_cell: "ScalableCell"):
        """
        Args:
            scalable_cell: the scalable cell to be deployed.
        Raises:
            InvalidInputException
            NotDeployableException – if the internal cell cannot be deployed.
            AlreadyDeployedException – if the internal cell is already deployed.
            NotDeployableException – if the internal cell cannot be deployed.
            Exception – any exception that the user may raise when overriding _on_deploy or _on_undeploy
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Creating ScalableCellDeployment...")

        # Validate the given scalable cell
        raise_if_not(self.can_have_as_scalable_cell(scalable_cell), InvalidInputException)
        internal_cell = scalable_cell.get_internal_cell()
        if internal_cell is None:
            message = f"{scalable_cell}: Cannot deploy a scalable cell that has no internal cell to " \
                      f"scale. Make sure you add an internal cell to your scalable cell: \n" \
                      f"class {type(scalable_cell).__name__}(ScalableCell):\n" \
                      f"    def __init__(self, parent_cell: 'Optional[ACompositeCell]', name: str):\n" \
                      f"        super(B, self).__init__(parent_cell, name)\n" \
                      f"        self.internal_cell = SomeInternalCell(self, 'internal_cell_name')\n"
            raise NotDeployableException(message)
        raise_if_not(self.can_have_as_internal_cell(internal_cell), InvalidInputException)

        # Register the given scalable cell.
        # Main mutator in the ScalableCellDeployment-ScalableCell relation, as the deployment of the scalable cell.
        self.__scalable_cell = scalable_cell
        self.__scalable_cell._set_deployment(self)
        self.__internal_cell = internal_cell
        self.__is_deleted = False

        # Create the output queue
        queue_capacity = scalable_cell.config_queue_capacity.pull()
        queue_ids = scalable_cell.get_output_ports()
        output_queue: CircularMultiQueue[OutputPort, Any] = CircularMultiQueue(queue_ids, queue_capacity)
        assert self.can_have_as_output_queue(output_queue)
        self.__output_queue = output_queue

        # Create the scaling strategy
        self.logger.info(f"Creating pull strategy...")
        self.__scaling_strategy: AScalingStrategy
        scaling_strategy_type = self.get_scalable_cell().get_scaling_strategy_type()
        scaling_strategy_type.create(self)      # Should register itself as scaling strategy
        self.logger.info(f"Creating pull strategy done")

        if isinstance(self.__scaling_strategy, CloningStrategy):
            for clone_type in self.__scalable_cell.get_all_clone_types():
                self.scale_one_up(clone_type)

        self.logger.info(f"Creating ScalableCellDeployment done")

    # ----- Scaling strategy ------

    def get_scaling_strategy(self) -> AScalingStrategy:
        """
        Returns:
            The scaling strategy of this scalable cell.
        """
        return self.__scaling_strategy

    def _set_scaling_strategy(self, scaling_strategy: Optional[AScalingStrategy]) -> None:
        """
        Auxiliary mutator in the AScalingStrategy-ScalableCellDeployment relation, as the owner of the scaling strategy.

        Args:
            scaling_strategy: the new scaling strategy to set.
        Raises:
            InvalidInputException: if the given scaling strategy is not valid.
        """
        if scaling_strategy is not None:
            raise_if_not(self.can_have_as_scaling_strategy(scaling_strategy), InvalidInputException)
        self.__scaling_strategy = scaling_strategy

    @classmethod
    def can_have_as_scaling_strategy(cls, scaling_strategy: AScalingStrategy) -> BoolExplained:
        """
        Args:
            scaling_strategy: scaling strategy to validate.
        Returns:
            TrueExplained if the given scaling strategy is a valid scaling strategy for this deployment.
        """
        if not isinstance(scaling_strategy, AScalingStrategy):
            return FalseExplained(f"A scaling strategy must be an instance of an AScalingStrategy. "
                                  f"Got: {scaling_strategy}")
        return TrueExplained()

    def assert_has_proper_scaling_strategy(self) -> None:
        """
        Raises:
            InvalidStateException
        """
        scaling_strategy = self.get_scaling_strategy()
        raise_if_not(self.can_have_as_scaling_strategy(scaling_strategy), InvalidStateException)
        if scaling_strategy.get_scalable_cell_deployment() != self:
            raise InvalidStateException(f"Inconsistent relation: {scaling_strategy} doesn't have {self} as scalable "
                                        f"cell deployment. ")
        scaling_strategy.assert_is_valid()

    def switch_scaling_strategy(self, scaling_strategy: Type[AScalingStrategy]) -> None:
        """
        Switch between scaling strategies.

        Args:
            scaling_strategy: the new scaling strategy to switch to.
        """
        raise_if_not(self.can_have_as_scaling_strategy_type(scaling_strategy), InvalidInputException)
        if type(self.get_scaling_strategy()) == scaling_strategy:
            return
        self.__scaling_strategy.delete()   # delete the old strategy, should remove itself.
        scaling_strategy(self)          # create the new strategy, should register itself.

    @classmethod
    def can_have_as_scaling_strategy_type(cls, scaling_strategy_type: Type[AScalingStrategy]) -> BoolExplained:
        """
        Args:
            scaling_strategy_type: the scaling strategy type to validate.
        Returns:
            TrueExplained if the given type is a valid scaling strategy type for this scalable cell deployment.
        """
        if not issubclass(scaling_strategy_type, AScalingStrategy):
            return FalseExplained(f"The scaling strategy type of a scalable cell must be a type that subclasses "
                                  f"the AScalingStrategy type, got: {scaling_strategy_type}.")
        return TrueExplained()

    def scale_one_up(self, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        """
        Scale up the scalable cell with the given cloning method.

        Args:
            method: the cloning method to use to scale up the cell.
        Raises:
            ScalingNotSupportedException: if the strategy of the scalable cell doesn't allow scaling.
        """
        scaling_strategy = self.get_scaling_strategy()
        scaling_strategy.add_clone(method)

    def scale_one_down(self, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        """
        Scale down the scalable cell by removing a clone with the given type.

        Args:
            method: the cloning method to remove from this scalable cell.
        Raises:
            ScalingNotSupportedException: if the strategy of the scalable cell doesn't allow scaling.
        """
        scaling_strategy = self.get_scaling_strategy()
        scaling_strategy.remove_clone(method)

    # ------ Scalable Cell ------

    def get_scalable_cell(self) -> "ScalableCell":
        """
        Returns:
            The scalable cell to which this ScalableCellDeployment belongs.
        """
        return self.__scalable_cell

    @classmethod
    def can_have_as_scalable_cell(cls, scalable_cell: "ScalableCell") -> BoolExplained:
        """
        Args:
            scalable_cell: the scalable cell to validate.
        Returns:
            TrueExplained if the given scalable cell is a valid scalable cell for this ScalableCellDeployment.
            FalseExplained otherwise.
        """
        if not isinstance(scalable_cell, pypipeline.cell.compositecell.scalablecell.ScalableCell):
            return FalseExplained(f"A ScalableCellDeployment needs a ScalableCell to be linked with, "
                                  f"got {scalable_cell}.")
        return TrueExplained()

    def get_internal_cell(self) -> "ICell":
        """
        Returns:
            The internal cell of the scalable cell, i.e. the cell to scale up.
        """
        return self.__internal_cell

    @classmethod
    def can_have_as_internal_cell(cls, cell: "ICell") -> BoolExplained:
        """
        Args:
            cell: the cell to validate.
        Returns:
            TrueExplained if the given cell is a valid internal cell for this ScalableCellDeployment.
        """
        if not isinstance(cell, pypipeline.cell.icell.ICell):
            return FalseExplained(f"The internal cell of a ScalableCell must be an instance of the ICell interface, "
                                  f"got {cell}.")
        return TrueExplained()

    def assert_has_proper_scalable_cell(self) -> None:
        """
        Raises:
            InvalidStateException: if this scalable cell deployment is in invalid state.
        """
        raise_if_not(self.can_have_as_scalable_cell(self.get_scalable_cell()), InvalidStateException)
        raise_if_not(self.can_have_as_internal_cell(self.get_internal_cell()), InvalidStateException)
        if self.get_scalable_cell()._get_deployment() != self:      # access to protected member on purpose
            raise InvalidStateException(f"Inconsistent relation: {self.get_scalable_cell()} doesn't have {self} "
                                        f"as scalable cell deployment. ")

    # ------ Output queue ------

    def get_output_queue(self) -> CircularMultiQueue[OutputPort, Any]:
        """
        Returns:
            The output queue related to this scalable cell deployment.
        """
        return self.__output_queue

    def can_have_as_output_queue(self, output_queue: Optional[CircularMultiQueue[OutputPort, Any]]) -> BoolExplained:
        """
        Args:
            output_queue: the output queue to validate.
        Returns:
            TrueExplained if the given output queue is a valid output queue for this scalable cell deployment.
            FalseExplained otherwise.
        """
        if not isinstance(output_queue, CircularMultiQueue):
            return FalseExplained(f"The output queue of a scalable cell deployment should be of type "
                                  f"'CircularMultiQueue', got {type(output_queue)}.")
        if set(self.get_scalable_cell().get_output_ports()) != set(output_queue.get_queue_ids()):
            return FalseExplained(f"The output queue IDs don't match the output ports of this scalable cell.")
        return TrueExplained()

    def assert_has_proper_output_queue(self) -> None:
        """
        Raises:
            InvalidStateException: if the output queue is invalid.
        """
        raise_if_not(self.can_have_as_output_queue(self.get_output_queue()), InvalidStateException)

    # ------ General methods ------

    def _on_pull(self) -> None:
        """
        Executed when the scalable cell gets pulled.

        Raises:
            NotDeployedException: when the scalable cell is not deployed.
            InactiveException: when the scalable cell is inactive (ex: it has no active scale-up worker threads).
            # TODO may raise exceptions
        """
        self.get_scaling_strategy()._on_pull()     # TODO may raise exception
        # The strategies write their results to the result queue.
        # So wait here until the next result in the result queue is ready.
        # Raises a StopIteration when end of the queue is reached
        self.logger.debug(f"{self} strategy has been pulled, waiting for queue item")
        timeout = self.get_scalable_cell().PULL_TIMEOUT
        queue_items: Dict[OutputPort, Any]
        while True:
            try:
                queue_items = self.get_output_queue().wait_and_pop(timeout=timeout)
            except TimeoutException:
                self.logger.info(f"{self} waiting for queue results timed out")
                if self.__is_deleted:
                    raise NotDeployedException(f"{self} is being pulled, but got undeployed")
                elif not self.get_scaling_strategy().is_active():
                    raise InactiveException(f"{self} is being pulled, but got inactive. \nInactivity is caused by "
                                            f"having no (active) scale-up worker threads. Did you scale up this cell "
                                            f"at least once? -> {self.get_scalable_cell()}.scale_up()")
            else:
                break
        self.logger.debug(f"{self} got queue item")

        # When the result is ready, set the outputs
        for output_port, queue_item in queue_items.items():
            output_port.set_value(queue_item)

    def reset(self) -> None:
        """
        Reset the scalable cell deployment: reset the scaling strategy and the output queue.

        Raises:
            # TODO may raise exceptions
        """
        self.get_scaling_strategy().reset()
        self.get_output_queue().reset()

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if this scalable cell deployment is in invalid state.
        """
        self.assert_has_proper_output_queue()
        self.assert_has_proper_scalable_cell()
        self.assert_has_proper_scaling_strategy()

    def delete(self) -> None:
        """
        Delete this scalable cell deployment. Should only be used by the ScalableCell when undeploying.
        """
        self.get_scaling_strategy().delete()

        self.__output_queue = None      # type: ignore

        # Main mutator in the ScalableCellDeployment-ScalableCell relation, as the deployment of the scalable cell.
        self.__scalable_cell._set_deployment(None)      # access to protected member on purpose
        self.__scalable_cell = None     # type: ignore
        self.__internal_cell = None     # type: ignore

        self.__is_deleted = True

    def __str__(self) -> str:
        return f"{self.get_scalable_cell()}.{self.__class__.__name__}()"
