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

from typing import Optional, TYPE_CHECKING, Generator
import ray
import time
import pytest

from pypipeline.cell import ASingleCell, ScalableCell, RayCloneCell

if TYPE_CHECKING:
    from pypipeline.cell import ICompositeCell


@pytest.fixture
def ray_init_and_shutdown() -> Generator:
    ray.init()
    yield None
    ray.shutdown()


class CellA(ASingleCell):
    """
    A cell without any inputs
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(CellA, self).__init__(parent_cell, name=name)

    def supports_scaling(self) -> bool:
        return True

    def _on_pull(self) -> None:
        self.logger.warning(f"I'm executing :)")
        time.sleep(0.2)


class AScalableCell(ScalableCell):
    """
    A ScalableCell enables pipeline parallelism and multiple clones running in parallel processes.
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(AScalableCell, self).__init__(parent_cell, name=name)
        self.cell_a = CellA(self, "cell_a")

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "AScalableCell":
        return AScalableCell(new_parent, self.get_name())


def test_cloning_cell_without_io(ray_init_and_shutdown: None) -> None:
    a = AScalableCell(None, "a")
    a.config_queue_capacity.set_value(8)
    a.assert_is_valid()
    a.deploy()     # May be put before or after (or in between) the creations of the clone clones
    # a.scale_up(3, method=ThreadCloneCell)
    a.scale_up(3, method=RayCloneCell)
    a.assert_is_valid()

    for i in range(10):     # TODO a bit of a silly test...
        print(i)
        a.pull()

    a.undeploy()
    a.delete()
