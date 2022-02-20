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

from typing import TYPE_CHECKING, Type
import logging

import pypipeline
from pypipeline.exceptions import InvalidStateException, InvalidInputException
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not

if TYPE_CHECKING:
    from pypipeline.cell.icell import ICell
    from pypipeline.cell.compositecell.scalablecell.scalablecelldeployment import ScalableCellDeployment
    from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell.iclonecell import ICloneCell


class AScalingStrategy:
    """
    Abstract base class for scaling strategies.

    A scaling strategy determines how a scalable cell should be executed. ATM 2 strategies are implemented:
     - the CloningStrategy (default): allows to scale up using clones of the internal cell
     - the NoScalingStrategy: doesn't allow scaling and executes the scalable cell's internal cell in the mainthread,
       just like a Pipeline. Useful for debugging.
    """

    def __init__(self, scalable_cell_deployment: "ScalableCellDeployment"):
        """
        Args:
            scalable_cell_deployment: the scalable cell deployment where this scaling strategy belongs to.
        Raises:
            NotDeployableException – if the internal cell cannot be deployed.
            AlreadyDeployedException – if the internal cell is already deployed.
            NotDeployableException – if the internal cell cannot be deployed.
            Exception – any exception that the user may raise when overriding _on_deploy or _on_undeploy
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        raise_if_not(self.can_have_as_scalable_cell_deployment(scalable_cell_deployment), InvalidInputException)
        scalable_cell_deployment._set_scaling_strategy(self)        # Access to protected method on purpose
        self.__scalable_cell_deployment = scalable_cell_deployment

    @classmethod
    def create(cls, scalable_cell_deployment: "ScalableCellDeployment") -> "AScalingStrategy":
        """
        Factory method for creating a scaling strategy.

        Args:
            scalable_cell_deployment: the scalable cell deployment to create this scaling strategy on.
        Returns:
            The newly created scaling strategy.
        Raises:
            NotDeployableException – if the internal cell cannot be deployed.
            AlreadyDeployedException – if the internal cell is already deployed.
            NotDeployableException – if the internal cell cannot be deployed.
            Exception – any exception that the user may raise when overriding _on_deploy or _on_undeploy
        """
        raise NotImplementedError

    def get_scalable_cell_deployment(self) -> "ScalableCellDeployment":
        """
        Returns:
            The scalable cell deployment object to which this scaling strategy is linked.
        """
        return self.__scalable_cell_deployment

    def can_have_as_scalable_cell_deployment(self, sc_deployment: "ScalableCellDeployment") -> "BoolExplained":
        """
        Args:
            sc_deployment: scalable cell deployment object to validate.
        Returns:
            TrueExplained if the given deployment object is a valid scalable cell deployment for this scaling strategy.
             FalseExplained otherwise.
        """
        if not isinstance(sc_deployment, pypipeline.cell.compositecell.scalablecell.ScalableCellDeployment):
            return FalseExplained(f"{self} expected a deployment object of type ScalableCellDeployment, got "
                                  f"{sc_deployment}")
        return TrueExplained()

    def assert_has_proper_scalable_cell_deployment(self) -> None:
        """
        Raises:
            InvalidStateException: if the scalable cell deployment to which this strategy is linked is not valid.
        """
        deployment = self.get_scalable_cell_deployment()
        raise_if_not(self.can_have_as_scalable_cell_deployment(deployment), InvalidStateException)
        if deployment.get_scaling_strategy() != self:
            raise InvalidStateException(f"Inconsistent relation: {deployment} doesn't have {self} as scaling strategy.")

    def get_internal_cell(self) -> "ICell":
        """
        Returns:
            The internal cell of the scalable cell to which this scaling strategy is linked.
        """
        return self.get_scalable_cell_deployment().get_internal_cell()

    def add_clone(self, method: Type["ICloneCell"]) -> None:
        """
        Add a clone with the given type to this scaling strategy.

        Args:
            method: the clone type to create o new clone with.
        Raises:
            ScalingNotSupportedException: if this strategy doesn't support scaling.
        """
        raise NotImplementedError

    def remove_clone(self, method: Type["ICloneCell"]) -> None:
        """
        Remove a clone with the given type from this scaling strategy.

        Args:
            method: the clone type to remove.
        Raises:
            ScalingNotSupportedException: if this strategy doesn't support scaling.
        """
        raise NotImplementedError

    def _on_pull(self) -> None:
        """
        Executed when the scalable cell gets pulled.

        Raises:
            InactiveException: when the scalable cell is inactive (ex: it has no active scale-up worker threads).
            # TODO may raise exceptions
        Doesn't raise:
            NotDeployedException: this method can only be called when the scalable cell is deployed.
        """
        raise NotImplementedError

    def reset(self) -> None:
        """
        Reset the scaling strategy and its internal cell.

        Raises:
            # TODO may raise exceptions
        """
        raise NotImplementedError

    def is_active(self) -> bool:
        """
        Returns:
            Whether the scaling strategy is actively pulling right now.
        """
        return True

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if one of the attributes or relations of this scaling strategy is invalid.
        """
        self.assert_has_proper_scalable_cell_deployment()

    def delete(self) -> None:
        """
        Deletes this scaling strategy.

        Raises:
            NotDeployedException – if this cell is not deployed.
            Exception – any exception that the user may raise when overriding _on_undeploy
        """
        self.__scalable_cell_deployment._set_scaling_strategy(None)     # Access to protected method on purpose
        self.__scalable_cell_deployment = None

    def __str__(self) -> str:
        return f"{str(self.__scalable_cell_deployment.get_scalable_cell())}.{self.__class__.__name__}"
