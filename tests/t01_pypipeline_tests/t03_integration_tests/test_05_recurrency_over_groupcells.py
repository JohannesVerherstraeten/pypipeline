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

from typing import Optional, List, Type, Generator
import pytest
import ray

from pypipeline.cell import Pipeline, ScalableCell, ASingleCell, ICompositeCell, ICloneCell, ThreadCloneCell, \
    RayCloneCell
from pypipeline.cell.compositecell.scalablecell.strategy import NoScalingStrategy
from pypipeline.cellio import Output, Input, InputPort, OutputPort
from pypipeline.connection import Connection

import logging


@pytest.fixture
def ray_init_and_shutdown() -> Generator:
    ray.init()
    yield None
    ray.shutdown()


# Recurrent connections and scalable-cells with pipeline parallelism. A lot can go wrong here.
# This combination forces us to have a very good synchronization between the parallel processes.
# Imagine for example having a scalable-cell with a recurrent connection around it, and scaling this scalable-cell up.
# Logically, the upscaling should not have any benefits here, since the scalable-cell can only execute again when
# its previous execution has fully finished.
# This example shows that our implementation safely handles this!
# Note that the logs may appear out of order because of the multiprocessing and logging not syncing very well,
# but the cell never skips a value or produces the same value twice! -> The clones are forced to wait for each
# other because of the recurrent connection.


counter_list: List[int] = []        # global variable... :(


class ConsumerCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ConsumerCell, self).__init__(parent_cell, name=name)
        self.input: Input[int] = Input(self, "input")
        self.output: Output[int] = Output(self, "output")

    def _on_pull(self) -> None:
        val: int = self.input.pull()
        counter_list.append(val)
        self.output.set_value(val + 1)

    def supports_scaling(self) -> bool:
        return True


class ConsumerScalableCell(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ConsumerScalableCell, self).__init__(parent_cell, name=name)

        self.comsumercell = ConsumerCell(self, "consumer_cell")

        self.input_port: InputPort[int] = InputPort(self, "input_port")
        self.output_port: OutputPort[int] = OutputPort(self, "output_port", initial_value=0)

        Connection(self.input_port, self.comsumercell.input)
        Connection(self.comsumercell.output, self.output_port)

        self.config_queue_capacity.set_value(6)


class ToplevelPipeline1(Pipeline):

    def __init__(self):
        super(ToplevelPipeline1, self).__init__(None, "toplevel")

        self.scalable_cell = ConsumerScalableCell(self, "scalable_cell_1")

        # Recurrent connection over the scalable cell
        Connection(self.scalable_cell.output_port, self.scalable_cell.input_port)


@pytest.fixture
def toplevel_pipeline1() -> ToplevelPipeline1:
    return ToplevelPipeline1()


def test_1_without_upscaling(toplevel_pipeline1: ToplevelPipeline1) -> None:
    global counter_list
    counter_list = []

    p = toplevel_pipeline1
    p.scalable_cell.set_scaling_strategy_type(NoScalingStrategy)
    p.assert_is_valid()
    p.deploy()

    for i in range(100):
        p.pull()

    for i, counter_value in enumerate(counter_list):
        assert i == counter_value

    p.undeploy()
    p.delete()


@pytest.mark.parametrize("scaleup_method", [ThreadCloneCell, RayCloneCell])
def test_1_with_upscaling(ray_init_and_shutdown: None,
                          toplevel_pipeline1: ToplevelPipeline1,
                          scaleup_method: Type[ICloneCell]) -> None:
    global counter_list
    counter_list = []

    p = toplevel_pipeline1
    p.assert_is_valid()

    p.scalable_cell.scale_up(3, scaleup_method)
    p.deploy()

    for i in range(100):
        p.pull()

    for i, counter_value in enumerate(counter_list):
        assert i == counter_value

    p.undeploy()
    p.delete()


class ToplevelPipeline2(Pipeline):

    def __init__(self):
        super(ToplevelPipeline2, self).__init__(None, "toplevel")

        self.cell1_scalable = ConsumerScalableCell(self, "cell1_scalable")
        self.cell2_scalable = ConsumerScalableCell(self, "cell2_scalable")

        Connection(self.cell1_scalable.output_port, self.cell2_scalable.input_port)
        Connection(self.cell2_scalable.output_port, self.cell1_scalable.input_port, explicitly_mark_as_recurrent=True)


@pytest.fixture
def toplevel_pipeline2() -> ToplevelPipeline2:
    return ToplevelPipeline2()


def test_2_without_upscaling(toplevel_pipeline2: ToplevelPipeline2) -> None:
    global counter_list
    counter_list = []

    p = toplevel_pipeline2
    p.cell1_scalable.set_scaling_strategy_type(NoScalingStrategy)
    p.cell2_scalable.set_scaling_strategy_type(NoScalingStrategy)
    p.assert_is_valid()
    p.deploy()

    for i in range(100):
        p.pull()

    for i, counter_value in enumerate(counter_list):
        assert i == counter_value

    p.undeploy()
    p.delete()


@pytest.mark.parametrize("scaleup_method", [ThreadCloneCell, RayCloneCell])
def test_2_with_upscaling(ray_init_and_shutdown: None,
                          toplevel_pipeline2: ToplevelPipeline2,
                          scaleup_method: Type[ICloneCell]) -> None:
    global counter_list
    counter_list = []

    p = toplevel_pipeline2
    p.assert_is_valid()

    p.cell1_scalable.scale_up(3, scaleup_method)
    p.cell2_scalable.scale_up(3, scaleup_method)
    p.deploy()

    for i in range(100):
        p.pull()

    for i, counter_value in enumerate(counter_list):
        assert i == counter_value

    p.undeploy()
    p.delete()
