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

import logging
from typing import Optional, TYPE_CHECKING, Generator
import time
import random
import ray
import pytest

from pypipeline.cell import ASingleCell, ScalableCell, Pipeline, RayCloneCell, ThreadCloneCell
from pypipeline.cellio import Input, Output, OutputPort, InputPort
from pypipeline.connection import Connection

if TYPE_CHECKING:
    from pypipeline.cell import ICompositeCell


@pytest.fixture
def ray_init_and_shutdown() -> Generator:
    ray.shutdown()
    ray.init()
    yield None
    ray.shutdown()


class CellA(ASingleCell):
    """
    A source cell providing data.
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(CellA, self).__init__(parent_cell, name=name)
        self.output1: Output[float] = Output(self, "output1")
        self.output2: Output[int] = Output(self, "output2")
        self.counter: int = 1

    def supports_scaling(self) -> bool:     # This cell has internal state, so should not be scaled up
        return False

    def _on_pull(self) -> None:
        self.output1.set_value(self.counter / 10.)    # typechecks!
        self.output2.set_value(self.counter)
        self.counter += 1

    def _on_reset(self) -> None:
        super(CellA, self)._on_reset()
        self.counter = 1


class CellB(ASingleCell):
    """
    Heavy computation cell. (see sleep)
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(CellB, self).__init__(parent_cell, name=name)
        self.input1: Input[float] = Input(self, "input1")
        self.input2: Input[int] = Input(self, "input2")
        self.output: Output[float] = Output(self, "output")

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        value1: float = self.input1.pull()     # typechecks!
        value2: int = self.input2.pull()
        result_sum = value1 + value2
        time.sleep(random.random() * 0.10)     # a heavy computation
        # print(f"[CellB]: log: {result_sum}")
        self.logger.debug(f"setting value: {result_sum}")
        self.output.set_value(result_sum)


def test_without_upscaling() -> None:
    # print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    # first test the ACell and BCell without parallelization or scaling:
    toplevel_pipeline = Pipeline(None, "toplevel")

    cell_a = CellA(toplevel_pipeline, "cell_a")
    cell_b = CellB(toplevel_pipeline, "cell_b")

    Connection(cell_a.output1, cell_b.input1)                           # typechecks
    Connection(cell_a.output2, cell_b.input2)                           # typechecks

    # No regulators, drivers or inference_processes needed, just pull the output you want.
    toplevel_pipeline.assert_is_valid()
    toplevel_pipeline.deploy()
    expected_outputs = [i/10. + i for i in range(1, 6)]
    for expected_output in expected_outputs:
        assert cell_b.output.pull() == expected_output
    toplevel_pipeline.assert_is_valid()
    toplevel_pipeline.undeploy()
    toplevel_pipeline.delete()

# print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
# now test the CellA and CellB in an upscaleable ScalableCell:
# Note that the scalablecell workers may raise exceptions on exit. Todo gracefull thread exiting.


class BScalableCell(ScalableCell):
    """
    A ScalableCell enables pipeline parallelism and multiple clones running in parallel processes.
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(BScalableCell, self).__init__(parent_cell, name=name)
        self.cell_b = CellB(self, "cell_b")

        self.input_port_1: InputPort[float] = InputPort(self, "input1")
        self.input_port_2: InputPort[int] = InputPort(self, "input2")
        self.output_port: OutputPort[float] = OutputPort(self, "output")

        Connection(self.input_port_1, self.cell_b.input1)
        Connection(self.input_port_2, self.cell_b.input2)
        Connection(self.cell_b.output, self.output_port)


class ToplevelPipeline(Pipeline):
    """
    Note: this is totally equivalent to the following, only implemented as subclass of a Pipeline
    (which is a bit more structured and therefore the recommended way).

    toplevel_pipeline = Pipeline(None, "toplevel")

    cell_a = CellA(toplevel_pipeline, "cell_a")
    cell_b_scalable = BScalableCell(toplevel_pipeline, "cell_b_scalable")

    output_port: OutputPort[float] = OutputPort(toplevel_pipeline, "pipeline_output")

    Connection(cell_a.output1, cell_b_scalable.input_port_1)                 # typechecks
    Connection(cell_a.output2, cell_b_scalable.input_port_2)                 # typechecks
    Connection(cell_b_scalable.output_port, output_port)                     # typechecks
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ToplevelPipeline, self).__init__(parent_cell, name=name)

        self.cell_a = CellA(parent_cell=self, name="cell_a")
        self.cell_b_scalable = BScalableCell(parent_cell=self, name="cell_b_scalable")

        self.output_port: OutputPort[float] = OutputPort(self, "pipeline_output")

        Connection(self.cell_a.output1, self.cell_b_scalable.input_port_1)                 # typechecks
        Connection(self.cell_a.output2, self.cell_b_scalable.input_port_2)                 # typechecks
        Connection(self.cell_b_scalable.output_port, self.output_port)                    # typechecks


@pytest.mark.parametrize("do_reset", [
    True,
    False
])
def test_with_upscaling(ray_init_and_shutdown: None, do_reset: bool) -> None:
    # Add 4 clone clones: 4 parallel processes executing the clone.
    # We set the queue capacity of the scalable-cell high enough, such that all 4 processes can write their result to this
    # queue and don't have to wait for us to pull a value out of this queue before they can start their new run.

    ab = ToplevelPipeline(None, "toplevel")

    ab.cell_b_scalable.config_queue_capacity.set_value(5)
    ab.cell_b_scalable.config_check_quit_interval.set_value(0.5)
    ab.assert_is_valid()
    ab.deploy()
    ab.assert_is_valid()

    expected_outputs = [i / 10. + i for i in range(1, 18)]
    actual_output = None

    for i, scaling_method in enumerate([ThreadCloneCell, RayCloneCell, ThreadCloneCell]):
        ab.cell_b_scalable.scale_up(4, scaling_method)
        ab.assert_is_valid()

        expected_outputs_this_iteration = expected_outputs[:6] if do_reset else expected_outputs[i*6: (i+1)*6]

        for expected_output in expected_outputs_this_iteration:
            ab.logger.info(f"Pulling for expected output {expected_output}...")
            ab.pull()
            previous_actual_output = actual_output
            actual_output = ab.output_port.get_value()
            ab.logger.info(f"Actual output: {actual_output}, expected output: {expected_output}")
            if not do_reset:
                # Downscaling while a scalablecell is active, may result in data loss.
                # However, the ordering of the results remains kept
                assert previous_actual_output is None or actual_output > previous_actual_output
            else:
                assert actual_output == expected_output, f"{actual_output} == {expected_output}"

        ab.logger.info(f"Start assert_is_valid")
        ab.assert_is_valid()
        ab.logger.info(f"Start scale_all_down")
        ab.cell_b_scalable.scale_all_down()
        if do_reset:
            ab.logger.info(f"Start reset")
            ab.reset()
    ab.undeploy()
    ab.delete()
    logging.info(f"Done")


# # first pull will be slow, since the data must pass the whole pipeline
# # (heavy computation duration is 0.5s)
# # next pull will be fast, because of the extra 3 running processes
#
# # fifth pull will be slower again, but sometimes still less than 0.5s
# # because when the first clone finished its first run,
# # it immediately started a next run, before we executed the fifth
# # pull (=pipeline parallelization).
