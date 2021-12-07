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

from pypipeline.cell.icellobserver import ScalingStrategyUpdateEvent, CloneTypeUpdateEvent
from pypipeline.cell.compositecell.acompositecell import ACompositeCell
from pypipeline.cell.compositecell.icompositecell import ICompositeCell
from pypipeline.cell.compositecell.scalablecell.scalablecelldeployment import ScalableCellDeployment
from pypipeline.cell.compositecell.scalablecell.strategy import AScalingStrategy, CloningStrategy
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ICloneCell, ThreadCloneCell
from pypipeline.cellio import OutputPort, InputPort, ConfigParameter
from pypipeline.exceptions import InvalidStateException

if TYPE_CHECKING:
    from pypipeline.cell.icell import ICell


class ScalableCell(ACompositeCell):

    __CONFIGURED_SCALING_STRATEGY_TYPE_KEY: str = "configured_scaling_strategy_type"
    __CONFIGURED_CLONE_TYPES_KEY: str = "configured_clone_types"

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ScalableCell, self).__init__(parent_cell, max_nb_internal_cells=1, name=name)
        self.__config_queue_capacity: ConfigParameter[int] = ConfigParameter[int](self, "queue_capacity",
                                                                                  self.__can_have_as_queue_capacity)
        self.__config_queue_capacity.set_value(2)
        self.__config_check_quit_interval: ConfigParameter[float] = \
            ConfigParameter[float](self, "check_quit_interval", self.__can_have_as_check_quit_interval)
        self.__config_check_quit_interval.set_value(2.)

        self.__configured_pull_strategy_type: Type[AScalingStrategy] = CloningStrategy
        self.__configured_clone_types: List[Type[ICloneCell]] = []

        self.__deployment: Optional[ScalableCellDeployment] = None

    # ------ Configuration ------

    @property
    def config_queue_capacity(self) -> ConfigParameter[int]:
        return self.__config_queue_capacity

    def __can_have_as_queue_capacity(self, queue_capacity: int) -> bool:
        return 1 <= queue_capacity

    @property
    def config_check_quit_interval(self) -> ConfigParameter[float]:     # TODO check wether can be replaced by PULL_TIMOUT
        return self.__config_check_quit_interval

    def __can_have_as_check_quit_interval(self, interval: float) -> bool:
        return 0 < interval

    # ------ Some easier accessors ------

    def get_internal_cell(self) -> "Optional[ICell]":
        internal_cell_list = list(self.get_internal_cells())
        if len(internal_cell_list) == 0:
            return None
        return internal_cell_list[0]

    def get_input_ports(self) -> Sequence[InputPort]:
        return [input_ for input_ in self.get_inputs() if isinstance(input_, InputPort)]

    def get_output_ports(self) -> Sequence[OutputPort]:
        return [output for output in self.get_outputs() if isinstance(output, OutputPort)]

    # ------ Scaling strategy type ------

    def get_scaling_strategy_type(self) -> Type[AScalingStrategy]:
        return self.__configured_pull_strategy_type

    def set_scaling_strategy_type(self, pull_strategy: Type[AScalingStrategy]) -> None:
        assert self.can_have_as_scaling_strategy_type(pull_strategy)
        if self.__configured_pull_strategy_type == pull_strategy:
            return
        self.__configured_pull_strategy_type = pull_strategy
        event = ScalingStrategyUpdateEvent(self)
        self.notify_observers(event)
        deployment = self._get_deployment()
        if deployment is not None:
            deployment.switch_scaling_strategy(pull_strategy)    # access to protected method on purpose

    def can_have_as_scaling_strategy_type(self, pull_strategy: Type[AScalingStrategy]) -> bool:
        if not issubclass(pull_strategy, AScalingStrategy):
            return False
        return True

    def has_proper_scaling_strategy_type(self) -> bool:
        if not self.can_have_as_scaling_strategy_type(self.get_scaling_strategy_type()):
            return False
        return True

    # ------ Clone types ------

    def get_all_clone_types(self) -> Sequence[Type[ICloneCell]]:
        return tuple(self.__configured_clone_types)       # wrapping in a new tuple is needed for decoupling!

    def __add_clone_type(self, clone_type: Type[ICloneCell]) -> None:
        assert self.__can_have_as_clone_type(clone_type)
        self.__configured_clone_types.append(clone_type)
        event = CloneTypeUpdateEvent(self)
        self.notify_observers(event)
        deployment = self._get_deployment()
        if deployment is not None:
            deployment.scale_one_up(clone_type)     # TODO may raise exceptions

    def __remove_clone_type(self, clone_type: Type[ICloneCell]) -> None:
        assert self.__has_as_clone_type(clone_type)
        self.__configured_clone_types.remove(clone_type)
        event = CloneTypeUpdateEvent(self)
        self.notify_observers(event)
        deployment = self._get_deployment()
        if deployment is not None:
            deployment.scale_one_down(clone_type)       # TODO may raise exceptions

    def __can_have_as_clone_type(self, clone_type: Type[ICloneCell]) -> bool:
        return issubclass(clone_type, ICloneCell)

    def __has_as_clone_type(self, clone_type: Type[ICloneCell]) -> bool:
        return clone_type in self.__configured_clone_types

    def get_nb_clone_types(self) -> int:
        return len(self.__configured_clone_types)

    def get_nb_clone_types_by_type(self) -> Dict[Type[ICloneCell], int]:
        nb_clone_types_by_type: Dict[Type[ICloneCell], int] = {}
        for clone_type in self.get_all_clone_types():
            nb_clone_types_by_type[clone_type] = nb_clone_types_by_type.get(clone_type, 0) + 1
        return nb_clone_types_by_type

    def has_proper_clone_types(self) -> bool:
        for clone_type in self.get_all_clone_types():
            if not self.__can_have_as_clone_type(clone_type):
                return False
        return True

    # ------ Scaling ------

    def scale_up(self, times: int = 1, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        if times < 0:
            raise ValueError(f"The number of times a scalablecell is scaled up cannot be negative, got {times}")
        for i in range(times):
            self.logger.info(f"{self}: scaling up {i+1} of {times}")
            self.scale_one_up(method)

    def scale_one_up(self, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        # TODO test whether internal cell allows cloning/scaling up
        # TODO assert internal cell has no recurrent connections
        self.__add_clone_type(method)

    def scale_one_down(self, method: Type[ICloneCell] = ThreadCloneCell) -> None:
        self.__remove_clone_type(method)

    def scale_all_down(self) -> None:
        clone_types_to_scale_down = self.get_all_clone_types()
        for i, clone_type in enumerate(clone_types_to_scale_down):
            self.logger.info(f"Scaling down {i+1} of {len(clone_types_to_scale_down)}...")
            self.scale_one_down(clone_type)

    # ------ Deployment ------

    def _get_deployment(self) -> Optional[ScalableCellDeployment]:
        return self.__deployment

    def _can_have_as_deployment(self, deployment: Optional[ScalableCellDeployment]) -> bool:
        if deployment is None:
            return True
        # return isinstance(deployment, pypipeline.cell.compositecell.scalablecell.scalablecelldeployment)
        return True

    def _set_deployment(self, deployment: Optional[ScalableCellDeployment]) -> None:
        if not self._can_have_as_deployment(deployment):
            raise Exception()
        self.__deployment = deployment

    def _on_deploy(self) -> None:
        super(ACompositeCell, self)._on_deploy()    # Skip the internal cell deployment
        ScalableCellDeployment(self)               # Sets itself as self.__deployment   # TODO may raise exceptions

    def _on_undeploy(self) -> None:
        super(ACompositeCell, self)._on_undeploy()  # Skip the internal cell undeployment
        self.__deployment.delete()              # removes itself as self.__deployment

    # ------ Pulling ------

    def _on_pull(self) -> None:
        deployment = self._get_deployment()
        assert deployment is not None
        deployment._on_pull()                   # TODO may raise exceptions

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

            pull_strategy = state[self.__CONFIGURED_SCALING_STRATEGY_TYPE_KEY]
            self.set_scaling_strategy_type(pull_strategy)

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
        if not self.has_proper_scaling_strategy_type():
            raise InvalidStateException()   # TODO
        if not self.has_proper_clone_types():
            raise InvalidStateException()   # TODO
        if self.is_deployed():
            deployment = self._get_deployment()
            if deployment is None:
                raise InvalidStateException()   # TODO
            deployment.assert_is_valid()
        elif self._get_deployment() is not None:
            raise InvalidStateException()       # TODO

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
