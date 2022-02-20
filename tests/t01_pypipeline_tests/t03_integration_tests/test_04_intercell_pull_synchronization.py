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

from typing import Optional
import time
import pytest

from pypipeline.cell import ICompositeCell, Pipeline, ScalableCell, RayCloneCell, ASingleCell
from pypipeline.cellio import Output, Input, InputPort, RuntimeParameter, OutputPort
from pypipeline.connection import Connection


# Multiple consumer cells must pull a source cell in sync. This means that a faster consumer cell must wait for the
# slower ones. Otherwise, the slower ones would not be able to keep the pace of the source cell generation.


class SourceCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SourceCell, self).__init__(parent_cell, name=name)
        self.output1: Output[int] = Output(self, "output1")
        self.output2: Output[int] = Output(self, "output2")
        self.counter: int = 0

    def _on_pull(self) -> None:
        self.output1.set_value(self.counter)
        self.output2.set_value(self.counter)
        self.counter += 1

    def supports_scaling(self) -> bool:
        return False


class ConsumerCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ConsumerCell, self).__init__(parent_cell, name=name)
        self.input: Input[int] = Input(self, "input")
        self.output: Output[int] = Output(self, "output")
        self.sleep_time: RuntimeParameter[float] = RuntimeParameter(self, "sleep_time")

    def _on_pull(self) -> None:
        val: int = self.input.pull()
        sleep_time = self.sleep_time.pull()
        time.sleep(sleep_time)
        self.output.set_value(val)

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "ConsumerCell":
        return ConsumerCell(new_parent, self.get_name())

    def supports_scaling(self) -> bool:
        return True


class ConsumerScalableCell(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ConsumerScalableCell, self).__init__(parent_cell, name=name)
        self.comsumercell = ConsumerCell(self, "consumercell")

        self.input_port: InputPort[int] = InputPort(self, "input_port")
        self.output_port: OutputPort[int] = OutputPort(self, "output_port")

        Connection(self.input_port, self.comsumercell.input)
        Connection(self.comsumercell.output, self.output_port)

        # A scalablecell tries to fill its output queue as soon as possible, only limited by its own
        # execution speed and the speed in which new inputs arrive.
        self.config_queue_capacity.set_value(6)


class ToplevelPipeline(Pipeline):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ToplevelPipeline, self).__init__(parent_cell, name=name)

        self.source = SourceCell(self, "source")
        self.consumer_1 = ConsumerScalableCell(self, "cell1_scalable")
        self.consumer_2 = ConsumerScalableCell(self, "cell2_scalable")
        self.consumer_3 = ConsumerScalableCell(self, "cell3_scalable")
        Connection(self.source.output1, self.consumer_1.input_port)
        Connection(self.source.output1, self.consumer_2.input_port)
        Connection(self.source.output2, self.consumer_3.input_port)


@pytest.fixture
def toplevel_pipeline() -> ToplevelPipeline:
    return ToplevelPipeline(None, "toplevel")


def test_slower_consumer_on_same_output(toplevel_pipeline: ToplevelPipeline) -> None:
    p = toplevel_pipeline

    p.consumer_1.comsumercell.sleep_time.set_value(0.0)
    p.consumer_2.comsumercell.sleep_time.set_value(0.1)
    p.consumer_3.comsumercell.sleep_time.set_value(0.0)

    p.consumer_1.scale_one_up()
    p.consumer_2.scale_one_up()
    p.consumer_3.scale_one_up()

    p.assert_is_valid()
    p.deploy()

    # At this moment, the consumer scalable cells are pulling their inputs as fast as possible to fill up their output
    # queue. If these consumers would not sync to follow the speed of the slowest one, the output queue of the faster
    # ones would fill up faster. This means that, OR the same producer value would occur multiple times in a row
    # in the queue of a faster cell, OR the queue of the slower cell would skip certain producer values.
    # Here we prove that the synchronization happens successfully by waiting until the queues of all consumer cells
    # have been filled, reading them out one by one, and making sure they contain the correct produced value (=counter).
    time.sleep(1.)

    for i in range(10):
        p.pull()
        assert p.consumer_1.output_port.get_value() == i
        assert p.consumer_2.output_port.get_value() == i
        assert p.consumer_3.output_port.get_value() == i

    p.undeploy()
    p.delete()


def test_slower_consumer_on_different_output(toplevel_pipeline: ToplevelPipeline) -> None:
    p = toplevel_pipeline

    p.consumer_1.comsumercell.sleep_time.set_value(0.0)
    p.consumer_2.comsumercell.sleep_time.set_value(0.0)
    p.consumer_3.comsumercell.sleep_time.set_value(0.1)

    p.consumer_1.scale_one_up()
    p.consumer_2.scale_one_up()
    p.consumer_3.scale_one_up()

    p.assert_is_valid()
    p.deploy()
    p.assert_is_valid()

    # At this moment, the consumer scalable cells are pulling their inputs as fast as possible to fill up their output
    # queue. If these consumers would not sync to follow the speed of the slowest one, the output queue of the faster
    # ones would fill up faster. This means that, OR the same producer value would occur multiple times in a row
    # in the queue of a faster cell, OR the queue of the slower cell would skip certain producer values.
    # Here we prove that the synchronization happens successful by waiting until the queues of all consumer cells
    # have been filled, reading them out one by one, and making sure they contain the correct produced value (=counter).
    time.sleep(1.)

    for i in range(8):
        p.pull()
        assert p.consumer_1.output_port.get_value() == i
        assert p.consumer_2.output_port.get_value() == i
        assert p.consumer_3.output_port.get_value() == i

    p.undeploy()
    p.delete()


# test_slower_consumer_on_same_output(ToplevelPipeline(None, "toplevel"))
