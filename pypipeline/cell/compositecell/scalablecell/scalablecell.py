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

"""
About the relation with a ScalableCell and its clones.

A ScalableCell keeps track of its configured
 - clone_types: the types of the clones that the user configured this ScalableCell to have. This can for example be
                   "two ThreadCloneCells and three RayCloneCells".
 - clones: the real clones that are DEPLOYED FROM THIS ScalableCell.

It is important to know that these don't always match: a ScalableCell can at inference time have multiple
clone_types configured, while not having any deployed clone.

Imagine the case of a ScalableCell B nested inside a ScalableCell A. Both are scaled up 2 times (=they will both have
2 clone_types). When ScalableCell A gets deployed, it will deploy its clone cells, but not its internal cell, as
this will only serve as a "proxy cell" which can used to remotely configure the B cell clones. (See the schematic
below).
A user can now, when ScalableCell A is deployed and running, for example add a new clone_type to ScalableCell B. This
will not cause the undeployed ScalableCell B (=the proxy) to create a new clone, but it will cause the observing
ScalableCell B clones to create a new clone.

In general, it must be that a DEPLOYED ScalableCell has deployed clones that match the clone_types,
and a NON-DEPLOYED ScalableCell has no deployed clones.

ScalableCell A:
 * deployed
 * internal cells:
   - ScalableCell B
     * not deployed!
     * internal cells: ...
     * clone_types: [Y, Y]
     * clones: None, because this cell is not deployed!
 * clone_types: [X, X]
 * clones:
   - X1
     - ScalableCell B (this is a clone, observing the original non-deployed ScalableCell B for configuration changes)
       * deployed
       * internal cells: ...
       * clone_types: [Y, Y]
       * clones:
         - Y1
         - Y2
   - X2
     - ScalableCell B (this is a clone, observing the original non-deployed ScalableCell B for configuration changes)
       * deployed
       * internal cells: ...
       * clone_types: [Y, Y]
       * clones:
         - Y3
         - Y4
"""
from typing import Optional, Dict, List, TYPE_CHECKING, Any, Sequence, Type
from prometheus_client import Histogram

from pypipeline.cell.icellobserver import ScalingStrategyUpdateEvent, CloneTypeUpdateEvent
from pypipeline.cell.compositecell.acompositecell import ACompositeCell
from pypipeline.cell.compositecell.icompositecell import ICompositeCell
from pypipeline.cell.compositecell.scalablecell.scalablecelldeployment import ScalableCellDeployment
from pypipeline.cell.compositecell.scalablecell.strategy import AScalingStrategy, CloningStrategy
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ICloneCell, ThreadCloneCell
from pypipeline.cellio import OutputPort, InputPort, ConfigParameter
from pypipeline.exceptions import InvalidStateException, InvalidInputException
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not

if TYPE_CHECKING:
    from pypipeline.cell.icell import ICell


class ScalableCell(ACompositeCell):

    __CONFIGURED_SCALING_STRATEGY_TYPE_KEY: str = "configured_scaling_strategy_type"
    __CONFIGURED_CLONE_TYPES_KEY: str = "configured_clone_types"

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        """
        Args:
            parent_cell: the cell in which this cell must be nested.
            name: name of this cell.
        Raises:
            InvalidInputException
        """
        super(ScalableCell, self).__init__(parent_cell, max_nb_internal_cells=1, name=name)
        self.__config_queue_capacity: ConfigParameter[int] = ConfigParameter[int](self, "queue_capacity",
                                                                                  self.__can_have_as_queue_capacity)
        self.__config_queue_capacity.set_value(2)
        self.__config_check_quit_interval: ConfigParameter[float] = \
            ConfigParameter[float](self, "check_quit_interval", self.__can_have_as_check_quit_interval)
        self.__config_check_quit_interval.set_value(2.)

        self.__configured_scaling_strategy_type: Type[AScalingStrategy] = CloningStrategy
        self.__configured_clone_types: List[Type[ICloneCell]] = []

        self.__deployment: Optional[ScalableCellDeployment] = None

    # ------ Configuration ------

    @property
    def config_queue_capacity(self) -> ConfigParameter[int]:
        """
        Returns:
            The configuration parameter for the output queue capacity of this scalable cell.
        """
        return self.__config_queue_capacity

    @staticmethod
    def __can_have_as_queue_capacity(queue_capacity: int) -> bool:
        """
        Args:
            queue_capacity: the queue capacity value to validate.
        Returns:
            True if the given value is a valid queue capacity.
        """
        return isinstance(queue_capacity, int) and 1 <= queue_capacity

    @property
    def config_check_quit_interval(self) -> ConfigParameter[float]:
        """
        Returns:
            The configuration parameter for the interval in which the clone threads, when requesting new inputs,
            will halt their request and check whether they are requested to quit.
        """
        return self.__config_check_quit_interval

    @staticmethod
    def __can_have_as_check_quit_interval(interval: float) -> bool:
        """
        Args:
            interval: the interval to validate.
        Returns:
            True if the given value is a valid interval.
        """
        return isinstance(interval, (int, float)) and 0 < interval

    # ------ Some easier accessors ------

    def get_internal_cell(self) -> "Optional[ICell]":
        """
        Returns:
            The internal cell of this scalable cell, if available.
        """
        internal_cell_list = self.get_internal_cells()
        if len(internal_cell_list) == 0:
            return None
        return internal_cell_list[0]

    def get_input_ports(self) -> Sequence[InputPort]:
        """
        Returns:
            All input ports of this scalable cell.
        """
        return [input_ for input_ in self.get_inputs() if isinstance(input_, InputPort)]

    def get_output_ports(self) -> Sequence[OutputPort]:
        """
        Returns:
            All output ports of this scalable cell.
        """
        return [output for output in self.get_outputs() if isinstance(output, OutputPort)]

    # ------ Scaling strategy type ------

    def get_scaling_strategy_type(self) -> Type[AScalingStrategy]:
        """
        Returns:
            The scaling strategy type of this scalable cell.
        """
        return self.__configured_scaling_strategy_type

    def set_scaling_strategy_type(self, scaling_strategy: Type[AScalingStrategy]) -> None:
        """
        TODO what happens when switching the scaling strategy from CloningStrategy to NoScalingStrategy when
            some clone_types are still configured?
        Args:
            scaling_strategy: the new scaling strategy to use.
        Raises:
            InvalidInputException: if the given value is not a valid scaling strategy.
        """
        raise_if_not(self.can_have_as_scaling_strategy_type(scaling_strategy), InvalidInputException, f"{self}: ")
        if not self.can_have_as_scaling_strategy_type(scaling_strategy):
            raise InvalidInputException(f"{self} cannot have {scaling_strategy} as pull strategy. "
                                        f"Expected a subclass of type AScalingStrategy.")
        if self.__configured_scaling_strategy_type == scaling_strategy:
            return
        self.__configured_scaling_strategy_type = scaling_strategy
        event = ScalingStrategyUpdateEvent(self)
        self.notify_observers(event)
        deployment = self._get_deployment()
        if deployment is not None:
            deployment.switch_scaling_strategy(scaling_strategy)    # access to protected method on purpose

    def can_have_as_scaling_strategy_type(self, scaling_strategy: Type[AScalingStrategy]) -> BoolExplained:
        """
        Args:
            scaling_strategy: scaling strategy to validate.
        Returns:
            TrueExplained if the given value is a valid scaling strategy, FalseExplained otherwise.
        """
        if not issubclass(scaling_strategy, AScalingStrategy):
            return FalseExplained(f"Expected scaling strategy type to be a subtype of AScalingStrategy, "
                                  f"got {scaling_strategy}.")
        return TrueExplained()

    def assert_has_proper_scaling_strategy_type(self) -> None:
        """
        Raises:
            InvalidStateException: if the scaling strategy type of this scalable cell is invalid.
        """
        raise_if_not(self.can_have_as_scaling_strategy_type(self.get_scaling_strategy_type()), InvalidStateException,
                     f"{self}: ")

    # ------ Clone types ------

    def get_all_clone_types(self) -> Sequence[Type[ICloneCell]]:
        """
        Returns:
            The clone types this scalable cell has been scaled-up with.
        """
        return tuple(self.__configured_clone_types)       # wrapping in a new tuple is needed for decoupling!

    def __add_clone_type(self, clone_type: Type[ICloneCell]) -> None:
        """
        Adds the given clone type to this scalable cell. If this scalable cell is deployed, it will create
        the clone immediately.

        Args:
            clone_type: the clone type to add.
        Raises:
            InvalidInputException: if the given clone_type is not a valid clone type.
            ScalingNotSupportedException: if the strategy of the scalable cell doesn't allow scaling.
        """
        raise_if_not(self.can_have_as_clone_type(clone_type), InvalidInputException, f"{self}: ")
        self.__configured_clone_types.append(clone_type)
        event = CloneTypeUpdateEvent(self)
        self.notify_observers(event)
        deployment = self._get_deployment()
        if deployment is not None:
            deployment.scale_one_up(clone_type)

    def __remove_clone_type(self, clone_type: Type[ICloneCell]) -> None:
        """
        Removes the given clone type from this scalable cell. If this scalable cell is deployed, it will delete
        the clone immediately.

        Args:
            clone_type: the clone type to remove.
        Raises:
            InvalidInputException: if this scalable cell doesn't have the given clone type available.
            ScalingNotSupportedException: if the strategy of the scalable cell doesn't allow scaling.
        """
        if not self.has_as_clone_type(clone_type):
            raise InvalidInputException(f"{self} doesn't have {clone_type} as clone type.")
        self.__configured_clone_types.remove(clone_type)
        event = CloneTypeUpdateEvent(self)
        self.notify_observers(event)
        deployment = self._get_deployment()
        if deployment is not None:
            deployment.scale_one_down(clone_type)

    def can_have_as_clone_type(self, clone_type: Type[ICloneCell]) -> BoolExplained:
        """
        Args:
            clone_type: the clone type to validate.
        Returns:
            TrueExplained if the given value is a valid clone type for this scalable cell.
        """
        if not issubclass(clone_type, ICloneCell):
            return FalseExplained(f"Expected the clone type to be a subclass of ICloneCell, got {clone_type}")
        return TrueExplained()

    def has_as_clone_type(self, clone_type: Type[ICloneCell]) -> bool:
        """
        Args:
            clone_type: a clone type.
        Returns:
            True if this scalable cell has at least one clone type of the given type.
        """
        return clone_type in self.__configured_clone_types

    def get_nb_clone_types(self) -> int:
        """
        Returns:
            The total number of clone types that are configured on this cell.
        """
        return len(self.__configured_clone_types)

    def get_nb_clone_types_by_type(self) -> Dict[Type[ICloneCell], int]:
        """
        Returns:
            The number of clone types that are configured on this cell, ordered by clone type.
        """
        nb_clone_types_by_type: Dict[Type[ICloneCell], int] = {}
        for clone_type in self.get_all_clone_types():
            nb_clone_types_by_type[clone_type] = nb_clone_types_by_type.get(clone_type, 0) + 1
        return nb_clone_types_by_type

    def assert_has_proper_clone_types(self) -> None:
        """
        Raises:
            InvalidStateException: if one of the clone types of this scalable cell is invalid.
        """
        for clone_type in self.get_all_clone_types():
            raise_if_not(self.can_have_as_clone_type(clone_type), InvalidStateException, f"{self}: ")

    # ------ Scaling ------

    def scale_up(self, times: int = 1, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        """
        Scale up this scalable cell by creating multiple clones of the internal cell.

        If this cell already has clones available, the new ones are just added.

        Args:
            times: how many extra clones should be generated.
            method: the method in which to create the clones, also called clone_type.
        Raises:
            InvalidInputException
            ScalingNotSupportedException: if the strategy of this scalable cell doesn't allow scaling.
        """
        if times < 0:
            raise InvalidInputException(f"{self}: the number of times a scalable cell is scaled up cannot be negative, "
                                        f"got {times}")
        for i in range(times):
            self.logger.info(f"{self}: scaling up {i+1} of {times}")
            self.scale_one_up(method)

    def scale_one_up(self, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        """
        Scale up this scalable cell by creating one extra clone of the internal cell.

        If this cell already has clones available, the new one is just added.

        Args:
            method: the method in which to create the clones, also called clone_type.
        Raises:
            InvalidInputException
            ScalingNotSupportedException: if the strategy of this scalable cell doesn't allow scaling.
        """
        # TODO test whether internal cell allows cloning/scaling up
        # TODO assert internal cell has no recurrent connections
        self.__add_clone_type(method)

    def scale_one_down(self, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        """
        Scale down this scalable cell by removing a clone of the given type.

        Args:
            method: the clone_type of which to remove a clone.
        Raises:
            InvalidInputException
            ScalingNotSupportedException: if the strategy of this scalable cell doesn't allow scaling.
        """
        self.__remove_clone_type(method)

    def scale_all_down(self) -> None:
        """
        Scale down this scalable cell by removing all its clones.

        Raises:
            ScalingNotSupportedException: if the strategy of this scalable cell doesn't allow scaling.
        """
        clone_types_to_scale_down = self.get_all_clone_types()
        for i, clone_type in enumerate(clone_types_to_scale_down):
            self.logger.info(f"Scaling down {i+1} of {len(clone_types_to_scale_down)}...")
            self.scale_one_down(clone_type)

    # ------ Deployment ------

    def _get_deployment(self) -> Optional[ScalableCellDeployment]:
        """
        Returns:
            The deployment context of this scalable cell.
        """
        return self.__deployment

    def _can_have_as_deployment(self, deployment: Optional[ScalableCellDeployment]) -> BoolExplained:
        """
        Args:
            deployment: an optional deployment object.
        Returns:
            True if the given deployment object is a valid deployment for this scalable cell.
        """
        if deployment is not None and not isinstance(deployment, ScalableCellDeployment):
            return FalseExplained(f"{self}: expected deployment of type ScalableCellDeployment, got {deployment}.")
        return TrueExplained()

    def _set_deployment(self, deployment: Optional[ScalableCellDeployment]) -> None:
        """
        Should only be used by the constructor / destructor of the ScalableCellDeployment class.
        Args:
            deployment: the deployment object to set.
        """
        raise_if_not(self._can_have_as_deployment(deployment), InvalidInputException)
        self.__deployment = deployment

    def _on_deploy(self) -> None:
        super(ACompositeCell, self)._on_deploy()    # Skip the internal cell deployment
        ScalableCellDeployment(self)                # Sets itself as self.__deployment

    def _on_undeploy(self) -> None:
        super(ACompositeCell, self)._on_undeploy()  # Skip the internal cell undeployment
        self.__deployment.delete()                  # removes itself as self.__deployment

    def assert_has_proper_deployment(self) -> None:
        """
        Raises:
            InvalidStateException: if the deployment of this scalable cell is invalid.
        """
        deployment = self._get_deployment()
        raise_if_not(self._can_have_as_deployment(self._get_deployment()), InvalidStateException)
        if self.is_deployed() and deployment is None:
            raise InvalidStateException(f"{self} is deployed, but doesn't have a deployment attribute.")
        if not self.is_deployed() and deployment is not None:
            raise InvalidStateException(f"{self} is not deployed, but has a deployment attribute.")
        if deployment is not None:
            deployment.assert_is_valid()

    # ------ Pulling ------

    def _create_pull_duration_metric(self) -> None:
        assert self._get_pull_duration_metric() is None
        metric = Histogram(f"pypipeline_{self.get_prometheus_name()}_pull_duration_seconds",
                           f"Duration of the pull calls of cell `{self.get_full_name()}`. ",
                           labelnames=["clone"],
                           registry=self._get_prometheus_metric_registry_unsafe())
        self._set_pull_duration_metric(metric)

    def _log_pull_duration(self, pull_time: float):
        # Pull duration in a ScalableCell is logged by the scaling strategy
        pass

    def _on_pull(self) -> None:
        deployment = self._get_deployment()
        assert deployment is not None
        deployment._on_pull()

    def _on_reset(self) -> None:
        deployment = self._get_deployment()
        assert deployment is not None
        deployment.reset()
        super(ACompositeCell, self)._on_reset()

    # ------ State synchronization ------

    def _get_sync_state(self) -> Dict[str, Any]:
        with self._get_pull_lock():
            state = super(ScalableCell, self)._get_sync_state()
            state[self.__CONFIGURED_SCALING_STRATEGY_TYPE_KEY] = self.get_scaling_strategy_type()
            state[self.__CONFIGURED_CLONE_TYPES_KEY] = self.get_nb_clone_types_by_type()
        return state

    def _set_sync_state(self, state: Dict) -> None:
        with self._get_pull_lock():
            super(ScalableCell, self)._set_sync_state(state)

            assert self.__CONFIGURED_SCALING_STRATEGY_TYPE_KEY in state
            assert self.__CONFIGURED_CLONE_TYPES_KEY in state

            scaling_strategy = state[self.__CONFIGURED_SCALING_STRATEGY_TYPE_KEY]
            self.set_scaling_strategy_type(scaling_strategy)

            # number of clones
            target_configured_clone_types: Dict[Type[ICloneCell], int] = state[self.__CONFIGURED_CLONE_TYPES_KEY]
            current_configured_clone_types: Dict[Type[ICloneCell], int] = self.get_nb_clone_types_by_type()

            for clone_type in set(target_configured_clone_types.keys()).union(current_configured_clone_types.keys()):
                diff = target_configured_clone_types.get(clone_type, 0) - \
                       current_configured_clone_types.get(clone_type, 0)
                if diff > 0:
                    for i in range(diff):
                        self.scale_one_up(clone_type)
                elif diff < 0:
                    for i in range(-diff):
                        self.scale_one_down(clone_type)

    # ------ General validation ------

    def assert_is_valid(self) -> None:
        super(ScalableCell, self).assert_is_valid()
        self.assert_has_proper_scaling_strategy_type()
        self.assert_has_proper_clone_types()
        self.assert_has_proper_deployment()

    # ------ Pickling ------

    def __getstate__(self) -> Dict:
        # called during pickling
        new_state = super(ScalableCell, self).__getstate__()
        new_state["_ScalableCell__deployment"] = None
        return new_state

    def __setstate__(self, state: Dict) -> None:
        # called during unpickling
        super(ScalableCell, self).__setstate__(state)
        self.__deployment = None
