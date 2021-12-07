# PyPipeline
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

from typing import Optional, Generator, Type
import time
import ray
import pytest
import logging

from pypipeline.cell import ASingleCell, ACompositeCell, ScalableCell, Pipeline, RayCloneCell, ThreadCloneCell, \
    ACloneCell
from pypipeline.cellio import Output, Input, InputPort, OutputPort
from pypipeline.connection import Connection
from pypipeline.exceptions import NonCriticalException


class RandomNonCriticalException(NonCriticalException):
    pass


class RandomCriticalException(Exception):
    pass


class SourceCell(ASingleCell):

    def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
        super(SourceCell, self).__init__(parent_cell, name)
        self.output_1: Output[int] = Output(self, "output_1")
        self.output_2: Output[int] = Output(self, "output_2")
        self.counter = 0

    def _on_pull(self) -> None:
        self.logger.warning(f"{self} being pulled...")
        time.sleep(0.01)
        # if self.counter == 4:
        #     raise SomeRandomException("some random error message")
        self.output_1.set_value(self.counter)
        self.output_2.set_value(self.counter)
        self.counter += 1


class SinkCell(ASingleCell):

    def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
        super(SinkCell, self).__init__(parent_cell, name)
        self.input_1: Input[int] = Input(self, "input_1")
        self.input_2: Input[int] = Input(self, "input_2")
        self.output: Output[int] = Output(self, "output")
        self.counter = 0

    def _on_pull(self) -> None:
        self.counter += 1
        value_1 = self.input_1.pull()
        if self.counter == 3:
            raise RandomNonCriticalException("Non-critical errors shouldn't stop the pipeline")
        elif self.counter == 6:
            raise RandomCriticalException("Critical errors should stop the pipeline")
        value_2 = self.input_2.pull()
        time.sleep(0.02)
        self.output.set_value(value_1 + value_2)


class ScalableSinkCell(ScalableCell):

    def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
        super(ScalableSinkCell, self).__init__(parent_cell, name)
        self.sink_cell = SinkCell(self, "sink_cell")

        self.input_1: InputPort[int] = InputPort(self, "input_port_1")
        self.input_2: InputPort[int] = InputPort(self, "input_port_2")
        self.output: OutputPort[int] = OutputPort(self, "output_port")

        Connection(self.input_1, self.sink_cell.input_1)
        Connection(self.input_2, self.sink_cell.input_2)
        Connection(self.sink_cell.output, self.output)


class TestPipeline(Pipeline):

    def __init__(self, parent_cell: Optional[ACompositeCell], name: str):
        super(TestPipeline, self).__init__(parent_cell, name)
        self.source = SourceCell(self, "source")
        self.sink = ScalableSinkCell(self, "scalable_sink")

        Connection(self.source.output_1, self.sink.input_1)
        Connection(self.source.output_2, self.sink.input_2)


@pytest.fixture
def ray_init_and_shutdown() -> Generator:
    ray.init()
    yield None
    ray.shutdown()


@pytest.mark.parametrize("scaleup_method", [ThreadCloneCell, RayCloneCell])
def test_exceptions_in_clones(ray_init_and_shutdown, scaleup_method: Type[ACloneCell]):
    pipeline = TestPipeline(None, "pipeline")
    pipeline.sink.scale_up(2, method=scaleup_method)
    pipeline.deploy()

    nb_noncritical_exceptions = 0

    for i in range(11):
        logging.warning(f"============= {i} =============")

        try:
            pipeline.pull()
        except RandomNonCriticalException as e:
            logging.warning(f"Got non-critical exception {e}")
            nb_noncritical_exceptions += 1
        except RandomCriticalException as e:
            logging.warning(f"Got critical exception {e}")
            # In theory the pipeline can arrive here with only one non-critical exception, if only one of the
            # clones did most of/all the work... Usually it should be 2
            assert nb_noncritical_exceptions == 1 or nb_noncritical_exceptions == 2
            break
        else:
            logging.warning(f"Got result {pipeline.sink.output.get_value()}")
            assert pipeline.sink.output.get_value() == 2 * i

    pipeline.undeploy()
