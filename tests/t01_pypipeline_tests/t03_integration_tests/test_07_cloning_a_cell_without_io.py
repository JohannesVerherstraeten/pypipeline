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
