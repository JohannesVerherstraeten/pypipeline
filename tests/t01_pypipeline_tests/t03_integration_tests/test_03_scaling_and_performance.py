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
Topology:

           _______      _______
Cell1  -> | Cell2 | -> | Cell3 | -> Cell4
           -------      -------
              1s           0.5s       0.25s

Where cell2 and cell3 are put in n scalable scalablecell.
"""
from typing import Optional, TYPE_CHECKING, Type, Generator
import time
import pytest
import ray

from pypipeline.cell import ASingleCell, Pipeline, ScalableCell, RayCloneCell, ThreadCloneCell, ICloneCell
from pypipeline.cellio import Input, Output, InputPort, OutputPort, RuntimeParameter, ConfigParameter
from pypipeline.connection import Connection


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
        self.int_increase.set_value(1)          # set default value
        self.float_increase: RuntimeParameter[float] = RuntimeParameter(self, "float_increase")
        self.float_increase.set_value(0.01)     # set default value

    def supports_scaling(self) -> bool:
        return False

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "Cell1":
        return Cell1(new_parent, self.get_name(), self.current_int, self.current_float)

    def _on_pull(self) -> None:
        """
        Cells should pull their inputs and set their outputs.
        """
        self.output1.set_value(self.current_int)
        self.output2.set_value(self.current_float)
        self.current_int += self.int_increase.pull()
        self.current_float += self.float_increase.pull()


class Cell2(ASingleCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell2, self).__init__(parent, name=name)
        self.input1: Input[int] = Input(self, "input1")
        self.input2: Input[float] = Input(self, "input2")
        self.output1: Output[str] = Output(self, "output1")
        self.output2: Output[str] = Output(self, "output2")
        self.config_sleep_time: ConfigParameter[float] = ConfigParameter(self, "sleep_time")
        self.config_sleep_time.set_value(0.2)
        self._sleep_time = None

    def supports_scaling(self) -> bool:
        return True

    def _on_deploy(self) -> None:
        super(Cell2, self)._on_deploy()
        self._sleep_time = self.config_sleep_time.pull()

    def _on_pull(self) -> None:
        """
        Cells should pull their inputs and set their outputs.
        """
        value1 = self.input1.pull()
        value2 = self.input2.pull()
        result = value1 + value2
        result_x2 = result * 2
        result1_str = str(result)
        result2_str = str(result_x2)
        time.sleep(self._sleep_time)   # heavy computation
        self.output1.set_value(result1_str)
        self.output2.set_value(result2_str)


class Cell3(ASingleCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell3, self).__init__(parent, name=name)
        self.input1: Input[str] = Input(self, "input1")
        self.input2: Input[str] = Input(self, "input2")
        self.output1: Output[str] = Output(self, "output1")
        self.output2: Output[str] = Output(self, "output2")
        self.config_sleep_time: ConfigParameter[float] = ConfigParameter(self, "sleep_time")
        self.config_sleep_time.set_value(0.1)
        self._sleep_time = None

    def supports_scaling(self) -> bool:
        return True

    def _on_deploy(self) -> None:
        super(Cell3, self)._on_deploy()
        self._sleep_time = self.config_sleep_time.pull()

    def _on_pull(self) -> None:
        """
        Cells should pull their inputs and set their outputs.
        """
        value1 = self.input1.pull()
        value2 = self.input2.pull()
        result1 = value2 + " - " + value1
        result2 = value2 + " + " + value1
        time.sleep(self._sleep_time)  # heavy computation
        self.output1.set_value(result1)
        self.output2.set_value(result2)


class Cell4(ASingleCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell4, self).__init__(parent, name=name)
        self.input1: Input[str] = Input(self, "input1")
        self.input2: Input[str] = Input(self, "input2")
        self.output: Output[str] = Output(self, "output")
        self.config_sleep_time: ConfigParameter[float] = ConfigParameter(self, "sleep_time")
        self.config_sleep_time.set_value(0.05)
        self._sleep_time = None

    def supports_scaling(self) -> bool:
        return True

    def _on_deploy(self) -> None:
        super(Cell4, self)._on_deploy()
        self._sleep_time = self.config_sleep_time.pull()

    def _on_pull(self) -> None:
        """
        Cells should pull their inputs and set their outputs.
        """
        value1 = self.input1.pull()
        value2 = self.input2.pull()
        time.sleep(self._sleep_time)
        result = f"({value1}, {value2}) == ({eval(value1)}, {eval(value2)})"
        self.output.set_value(result)


class PipelineWithoutScaling(Pipeline):

    def __init__(self) -> None:
        super(PipelineWithoutScaling, self).__init__(None, "pipeline_without_scaling")
        self.cell1 = Cell1(self, "cell1", start_int=1, start_float=0.001)
        self.cell2 = Cell2(self, "cell2")
        self.cell3 = Cell3(self, "cell3")
        self.cell4 = Cell4(self, "cell4")

        Connection(self.cell1.output1, self.cell2.input1)
        Connection(self.cell1.output2, self.cell2.input2)
        Connection(self.cell2.output1, self.cell3.input1)
        Connection(self.cell2.output2, self.cell3.input2)
        Connection(self.cell3.output1, self.cell4.input1)
        Connection(self.cell3.output2, self.cell4.input2)


@pytest.fixture()
def pipeline_without_scaling() -> PipelineWithoutScaling:
    return PipelineWithoutScaling()


class Cell2ScalableCell(ScalableCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell2ScalableCell, self).__init__(parent, name=name)
        self.inputport1: InputPort[int] = InputPort(self, "inputport1")
        self.inputport2: InputPort[float] = InputPort(self, "inputport2")
        self.outputport1: OutputPort[str] = OutputPort(self, "outputport1")
        self.outputport2: OutputPort[str] = OutputPort(self, "outputport2")

        self.cell2 = Cell2(self, "cell2")

        Connection(self.inputport1, self.cell2.input1)
        Connection(self.inputport2, self.cell2.input2)
        Connection(self.cell2.output1, self.outputport1)
        Connection(self.cell2.output2, self.outputport2)


class Cell3ScalableCell(ScalableCell):

    def __init__(self, parent: "Optional[ICompositeCell]", name: str):
        super(Cell3ScalableCell, self).__init__(parent, name=name)
        self.inputport1: InputPort[str] = InputPort(self, "inputport1")
        self.inputport2: InputPort[str] = InputPort(self, "inputport2")
        self.outputport1: OutputPort[str] = OutputPort(self, "outputport1")
        self.outputport2: OutputPort[str] = OutputPort(self, "outputport2")

        self.cell3 = Cell3(self, "cell3")

        Connection(self.inputport1, self.cell3.input1)
        Connection(self.inputport2, self.cell3.input2)
        Connection(self.cell3.output1, self.outputport1)
        Connection(self.cell3.output2, self.outputport2)


class PipelineWithScaling(Pipeline):

    def __init__(self) -> None:
        super(PipelineWithScaling, self).__init__(None, "pipeline_with_scaling")
        self.cell1 = Cell1(self, "cell1", start_int=1, start_float=0.001)
        self.cell2_scalable = Cell2ScalableCell(self, "cell2_scalable")
        self.cell3_scalable = Cell3ScalableCell(self, "cell3_scalable")
        self.cell4 = Cell4(self, "cell4")

        Connection(self.cell1.output1, self.cell2_scalable.inputport1)
        Connection(self.cell1.output2, self.cell2_scalable.inputport2)
        Connection(self.cell2_scalable.outputport1, self.cell3_scalable.inputport1)
        Connection(self.cell2_scalable.outputport2, self.cell3_scalable.inputport2)
        Connection(self.cell3_scalable.outputport1, self.cell4.input1)
        Connection(self.cell3_scalable.outputport2, self.cell4.input2)


@pytest.fixture()
def pipeline_with_scaling() -> PipelineWithScaling:
    return PipelineWithScaling()


def test_performance_without_scaling(pipeline_without_scaling: PipelineWithoutScaling) -> None:
    p = pipeline_without_scaling
    p.assert_is_valid()
    p.deploy()
    num_pulls = 50
    t0 = time.time()
    for i in range(num_pulls):
        p.pull()
    t1 = time.time()

    min_time = num_pulls * (p.cell2.config_sleep_time.get_value() +
                            p.cell3.config_sleep_time.get_value() +
                            p.cell4.config_sleep_time.get_value())
    actual_time = t1 - t0

    p.undeploy()
    p.delete()

    print(f"==========================================================================================================")
    print(f"Minimum time: {min_time}")
    print(f"Actual time: {actual_time}")
    print(f"==========================================================================================================")

    eps = 0.01
    assert (1 - eps) * min_time < actual_time < (1 + eps) * min_time


def test_performance_regression_without_scaling(pipeline_without_scaling: PipelineWithoutScaling) -> None:
    p = pipeline_without_scaling
    p.cell2.config_sleep_time.set_value(0)
    p.cell3.config_sleep_time.set_value(0)
    p.cell4.config_sleep_time.set_value(0)
    p.assert_is_valid()
    p.deploy()
    num_pulls = 10000
    t0 = time.time()
    for i in range(num_pulls):
        p.pull()
    t1 = time.time()

    p.undeploy()
    p.delete()

    # min_time = num_pulls * (p.cell2.sleep_time + p.cell3.sleep_time + p.cell4.sleep_time)
    actual_time = t1 - t0
    time_per_pull = actual_time / num_pulls
    pulls_per_second = num_pulls / actual_time

    print(f"==========================================================================================================")
    # print(f"Minimum time: {min_time}")
    print(f"Total time for {num_pulls} pulls: {actual_time}")
    print(f"Time per pull: {time_per_pull}")
    print(f"Pulls/second: {pulls_per_second}")
    print(f"==========================================================================================================")

    assert pulls_per_second > 3300, f"Got {pulls_per_second}"       # Expected value seems to range between 3400 - 3600


def performance_with_scaling(pipeline_with_scaling: PipelineWithScaling,
                             scaling_method: Type[ICloneCell],
                             eps: float = 0.01,
                             disable_sleeps: bool = False,
                             num_pulls: int = 50) -> None:
    p = pipeline_with_scaling
    if disable_sleeps:
        p.cell2_scalable.cell2.config_sleep_time.set_value(0)
        p.cell3_scalable.cell3.config_sleep_time.set_value(0)
        p.cell4.config_sleep_time.set_value(0)
    p.assert_is_valid()

    cell_2_scaleup_times = 4
    p.cell2_scalable.config_queue_capacity.set_value(cell_2_scaleup_times)
    p.cell2_scalable.scale_up(times=cell_2_scaleup_times, method=scaling_method)

    cell_3_scaleup_times = 2
    p.cell3_scalable.config_queue_capacity.set_value(cell_3_scaleup_times)
    p.cell3_scalable.scale_up(times=cell_3_scaleup_times, method=scaling_method)

    p.deploy()
    p.pull()        # Pull a first time already, as this call will be slower because of pipeline initialization

    t0 = time.time()
    for i in range(num_pulls):
        p.pull()
    t1 = time.time()

    p.undeploy()

    if not disable_sleeps:
        sleep_time_2 = p.cell2_scalable.cell2.config_sleep_time.get_value()
        sleep_time_3 = p.cell3_scalable.cell3.config_sleep_time.get_value()
        sleep_time_4 = p.cell4.config_sleep_time.get_value()

        min_time_no_scaling = num_pulls * (sleep_time_2 + sleep_time_3 + sleep_time_4)
        min_time_with_scaling = num_pulls * (sleep_time_2 / float(cell_2_scaleup_times) +
                                             sleep_time_3 / float(cell_3_scaleup_times) +
                                             sleep_time_4)
        min_time_with_scaling_and_pipelining = num_pulls * max(sleep_time_2 / float(cell_2_scaleup_times),
                                                               sleep_time_3 / float(cell_3_scaleup_times),
                                                               sleep_time_4)

    p.delete()

    actual_time = t1 - t0
    time_per_pull = actual_time / num_pulls
    pulls_per_second = num_pulls / actual_time

    print(f"==========================================================================================================")
    print(f"Scaling method: {str(scaling_method.__name__)}")
    if not disable_sleeps:
        print(f"Minimum time without scaling: {min_time_no_scaling}")
        print(f"Minimum time with current upscaling settings, but without pipeline parallelism: {min_time_with_scaling}")
        print(f"Minimum time with current upscaling settings and pipeline parallelism:"
              f" {min_time_with_scaling_and_pipelining}")
    print(f"Actual time: {actual_time}")
    print(f"Time per pull: {time_per_pull}")
    print(f"Pulls/second: {pulls_per_second}")
    print(f"==========================================================================================================")

    if not disable_sleeps:
        min_time = min_time_with_scaling_and_pipelining
        assert (1 - eps) * min_time < actual_time < (1 + eps) * min_time
    else:
        if scaling_method == ThreadCloneCell:
            assert pulls_per_second > 500       # expected value seems to range between 500 and 600
        else:
            assert pulls_per_second > 700       # expected value seems to range between 700 and 900


def test_performance_with_thread_scaling(pipeline_with_scaling: PipelineWithScaling) -> None:
    performance_with_scaling(pipeline_with_scaling, ThreadCloneCell, eps=0.1, num_pulls=100)


def test_performance_with_ray_scaling(pipeline_with_scaling: PipelineWithScaling, ray_init_and_shutdown: None) -> None:
    performance_with_scaling(pipeline_with_scaling, RayCloneCell, eps=0.1, num_pulls=100)


def test_performance_regression_with_thread_scaling(pipeline_with_scaling: PipelineWithScaling) -> None:
    performance_with_scaling(pipeline_with_scaling, ThreadCloneCell, eps=0.1, num_pulls=500, disable_sleeps=True)


def test_performance_regression_with_ray_scaling(pipeline_with_scaling: PipelineWithScaling, ray_init_and_shutdown: None) -> None:
    performance_with_scaling(pipeline_with_scaling, RayCloneCell, eps=0.1, num_pulls=500, disable_sleeps=True)
