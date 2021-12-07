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
When upscaling a scalablecell, multiple clones of the clonecell are created that run in parallel. When you want to
change a parameter of the clonecell, you want its change to be reflected in all clones as well.
This is non-trivial, as those clones may even run in a different python process (ex. ray Actor), to which you don't
have direct access as user.

For this reason, the concept of the dashboard cell is introduced. The dashboard cell is the clonecell inside the
scalablecell. Everytime the scalablecell scales up, a separate clone of this clonecell is made to a separate
thread/process. It's these clones that process the data, not the original clonecell. However, all clones are are
attached as observers to this original clonecell, and adjust their parameters accordingly when the corresponding
parameters in the original clonecell change. Therefore, changing the parameters of the original clonecell (the
dashboardcell) automatically updates the parameters of its clones as well. The dashboardcell functions as a kind of
dashboard which you can use to remotely control its clones.

This also works for nested scalable cells, which is what is tested here.
"""
from typing import Optional, TYPE_CHECKING, Type, Generator
import pytest
import ray

from pypipeline.cell import ASingleCell, Pipeline, ScalableCell, ThreadCloneCell, ICloneCell, RayCloneCell
from pypipeline.cellio import Input, Output, InputPort, OutputPort, RuntimeParameter
from pypipeline.connection import Connection
from pypipeline.exceptions import NotDeployableException


if TYPE_CHECKING:
    from pypipeline.cell import ICompositeCell


@pytest.fixture
def ray_init_and_shutdown() -> Generator:
    ray.init()
    yield None
    ray.shutdown()


class Cell1(ASingleCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str, start_int: int, start_float: float):
        super(Cell1, self).__init__(parent, name=name)
        self.output1: Output[int] = Output(self, "output1")
        self.output2: Output[float] = Output(self, "output2")
        self.current_int = start_int
        self.current_float = start_float
        self.int_increase: RuntimeParameter[int] = RuntimeParameter(self, "int_increase")
        self.int_increase.set_value(1)
        self.float_increase: RuntimeParameter[float] = RuntimeParameter(self, "float_increase")
        self.float_increase.set_value(0.01)

    def supports_scaling(self) -> bool:
        return False

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "Cell1":
        return Cell1(new_parent, self.get_name(), self.current_int, self.current_float)

    def _on_pull(self) -> None:
        """
        Cells should pull their inputs and set their outputs.
        """
        # self.logger.warning(f"Cell1 is being pulled")
        self.output1.set_value(self.current_int)
        self.output2.set_value(self.current_float)
        # self.logger.warning(f"Cell1 pull ready: outputs {self.current_int}, {self.current_float}")
        self.current_int += self.int_increase.pull()
        self.current_float += self.float_increase.pull()


class Cell2(ASingleCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell2, self).__init__(parent, name=name)
        self.clipping_value: RuntimeParameter[float] = RuntimeParameter(self, "clip_at")
        self.input1: Input[int] = Input(self, "input1")
        self.input2: Input[float] = Input(self, "input2")
        self.output1: Output[float] = Output(self, "output1")
        # self.output2: Output[str] = Output(self, "output2")

    def supports_scaling(self) -> bool:
        return True

    def _on_pull(self) -> None:
        """
        Cells should pull their inputs and set their outputs.
        """
        value1 = self.input1.pull()
        value2 = self.input2.pull()
        clip = self.clipping_value.pull()
        result = value1 + value2
        if result > clip:
            result = clip
        self.output1.set_value(result)


class Cell2ScalableCell(ScalableCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell2ScalableCell, self).__init__(parent, name=name)

        self.cell2 = Cell2(parent=self, name="cell_2")

        self.input1: InputPort[int] = InputPort(self, "cell2input1")
        self.input2: InputPort[float] = InputPort(self, "cell2input2")
        self.output1: OutputPort[float] = OutputPort(self, "cell2output1")
        # self.output2: OutputPort[str] = OutputPort(self, "cell2output2")

        Connection(self.input1, self.cell2.input1)
        Connection(self.input2, self.cell2.input2)

        Connection(self.cell2.output1, self.output1)
        # Connection(self.cell2.output2, self.output2)


class Cell2ScalableCell2(ScalableCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell2ScalableCell2, self).__init__(parent, name=name)

        self.cell2_scalable = Cell2ScalableCell(parent=self, name="inner_scalable_cell")

        self.cell2input1: InputPort[int] = InputPort(self, "cell22input1")
        self.cell2input2: InputPort[float] = InputPort(self, "cell22input2")
        self.cell2output1: OutputPort[float] = OutputPort(self, "cell22output1")

        Connection(self.cell2input1, self.cell2_scalable.input1)
        Connection(self.cell2input2, self.cell2_scalable.input2)

        Connection(self.cell2_scalable.output1, self.cell2output1)


class ToplevelPipeline(Pipeline):

    def __init__(self):
        super(ToplevelPipeline, self).__init__(None, "toplevel")

        self.cell1 = Cell1(self, "cell1", 1, 0.01)
        self.scalable_cell = Cell2ScalableCell2(self, "outer_scalable_cell")

        Connection(self.cell1.output1, self.scalable_cell.cell2input1)
        Connection(self.cell1.output2, self.scalable_cell.cell2input2)


@pytest.mark.parametrize("scaleup_method", [ThreadCloneCell, RayCloneCell])
def test_dashboard_cell_not_deployable(ray_init_and_shutdown, scaleup_method: Type[ICloneCell]) -> None:
    p = ToplevelPipeline()

    p.scalable_cell.config_queue_capacity.set_value(2)
    p.scalable_cell.cell2_scalable.config_queue_capacity.set_value(2)

    p.assert_is_valid()
    p.scalable_cell.scale_up(2, method=scaleup_method)
    p.scalable_cell.cell2_scalable.scale_up(2, method=scaleup_method)

    # p.scalable_cell.cell2_scalable.cell2.clipping_value.set_value(4.0)    # This is missing to be able to deploy

    p.assert_is_valid()
    with pytest.raises(NotDeployableException):
        p.deploy()


@pytest.mark.parametrize("scaleup_method", [ThreadCloneCell, RayCloneCell])
def test_dashboard_cell_good(ray_init_and_shutdown, scaleup_method: Type[ICloneCell]) -> None:
    p = ToplevelPipeline()

    p.scalable_cell.config_queue_capacity.set_value(2)
    p.scalable_cell.cell2_scalable.config_queue_capacity.set_value(2)

    p.assert_is_valid()
    p.scalable_cell.scale_up(2, method=scaleup_method)
    p.scalable_cell.cell2_scalable.scale_up(2, method=scaleup_method)

    p.scalable_cell.cell2_scalable.cell2.clipping_value.set_value(4.0)

    p.assert_is_valid()
    p.deploy()
    p.assert_is_valid()

    result_was_once_bigger_than_4 = False
    for i in range(10):
        print(i)
        if i == 6:
            p.scalable_cell.cell2_scalable.cell2.clipping_value.set_value(5.0)
        p.pull()
        result = p.scalable_cell.cell2output1.get_value()
        print(result)
        if i < 6:
            assert result <= 4.0
        elif 8 <= i:
            assert result <= 5.0
        else:
            # give the clones some time to update
            pass

        if result > 4.0:
            result_was_once_bigger_than_4 = True
    assert result_was_once_bigger_than_4

    p.undeploy()
    p.delete()
