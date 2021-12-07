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

from typing import TYPE_CHECKING, Type
import logging

if TYPE_CHECKING:
    from pypipeline.cell.icell import ICell
    from pypipeline.cell.compositecell.scalablecell.scalablecelldeployment import ScalableCellDeployment
    from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell.iclonecell import ICloneCell


class AScalingStrategy:

    def __init__(self, scalable_cell_deployment: "ScalableCellDeployment"):
        self.logger = logging.getLogger(self.__class__.__name__)
        scalable_cell_deployment._set_scaling_strategy(self)
        self.__scalable_cell_deployment = scalable_cell_deployment

    @classmethod
    def create(cls, scalable_cell_deployment: "ScalableCellDeployment") -> "AScalingStrategy":
        raise NotImplementedError

    def get_scalable_cell_deployment(self) -> "ScalableCellDeployment":
        return self.__scalable_cell_deployment

    def get_internal_cell(self) -> "ICell":
        return self.get_scalable_cell_deployment().get_internal_cell()

    def add_clone(self, method: Type["ICloneCell"]) -> None:
        raise NotImplementedError

    def remove_clone(self, method: Type["ICloneCell"]) -> None:
        raise NotImplementedError

    def _on_pull(self) -> None:
        raise NotImplementedError

    def reset(self) -> None:
        raise NotImplementedError

    def is_active(self) -> bool:
        return True

    def assert_is_valid(self) -> None:
        raise NotImplementedError

    def delete(self) -> None:
        self.__scalable_cell_deployment._set_scaling_strategy(None)
        self.__scalable_cell_deployment = None

    def __str__(self) -> str:
        return f"{str(self.__scalable_cell_deployment.get_scalable_cell())}.{self.__class__.__name__}"
