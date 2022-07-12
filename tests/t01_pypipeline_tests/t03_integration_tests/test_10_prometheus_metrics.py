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

from typing import Optional, Dict
import time
import pytest
from prometheus_client import start_http_server
import requests

from pypipeline.cell import ICompositeCell, Pipeline, ScalableCell, RayCloneCell, ASingleCell
from pypipeline.cellio import Output, Input, InputPort, RuntimeParameter, OutputPort
from pypipeline.connection import Connection


class SourceCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SourceCell, self).__init__(parent_cell, name=name)
        self.output: Output[int] = Output(self, "output1")
        self.sleep_time: RuntimeParameter[float] = RuntimeParameter(self, "sleep_time")
        self.counter: int = 0

    def _on_pull(self) -> None:
        sleep_time = self.sleep_time.pull()
        time.sleep(sleep_time)
        self.output.set_value(self.counter)
        self.counter += 1

    def supports_scaling(self) -> bool:
        return False


class DoubleConsumerCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DoubleConsumerCell, self).__init__(parent_cell, name=name)
        self.input: Input[int] = Input(self, "input")
        self.output: Output[int] = Output(self, "output")
        self.sleep_time: RuntimeParameter[float] = RuntimeParameter(self, "sleep_time")

    def _on_pull(self) -> None:
        value_sum = 0
        for _ in range(2):
            val: int = self.input.pull()
            value_sum += val
        sleep_time = self.sleep_time.pull()
        time.sleep(sleep_time)
        self.output.set_value(value_sum)

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "DoubleConsumerCell":
        return DoubleConsumerCell(new_parent, self.get_name())

    def supports_scaling(self) -> bool:
        return True


class ToplevelPipeline(Pipeline):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ToplevelPipeline, self).__init__(parent_cell, name=name)

        self.source = SourceCell(self, "source")
        self.consumer = DoubleConsumerCell(self, "double_consumer")
        Connection(self.source.output, self.consumer.input)


@pytest.fixture
def toplevel_pipeline() -> ToplevelPipeline:
    return ToplevelPipeline(None, "toplevel")


def test_prometheus_metrics(toplevel_pipeline: ToplevelPipeline) -> None:
    p = toplevel_pipeline

    SOURCE_SLEEP_TIME = 0.1
    CONSUMER_SLEEP_TIME = 0.1

    p.source.sleep_time.set_value(SOURCE_SLEEP_TIME)
    p.consumer.sleep_time.set_value(CONSUMER_SLEEP_TIME)

    p.assert_is_valid()
    p.deploy()

    PORT = 8809
    start_http_server(PORT, registry=p.get_prometheus_metric_registry())

    for i in range(5):
        p.pull()
        assert p.consumer.output.get_value() == 4 * i + 1
        response = requests.get(f"http://localhost:{PORT}/metrics")
        assert response.status_code == 200
        metrics: Dict[str, float] = dict()
        for line in response.text.splitlines():
            if line.startswith(f"pypipeline_"):
                metric_name, metric_value = line.split(" ")
                metrics[metric_name] = float(metric_value)

        toplevel_count = metrics[f"pypipeline_{p.get_prometheus_name()}_pull_duration_seconds_count"]
        source_count = metrics[f"pypipeline_{p.source.get_prometheus_name()}_pull_duration_seconds_count"]
        consumer_count = metrics[f"pypipeline_{p.consumer.get_prometheus_name()}_pull_duration_seconds_count"]

        toplevel_sum = metrics[f"pypipeline_{p.get_prometheus_name()}_pull_duration_seconds_sum"]
        source_sum = metrics[f"pypipeline_{p.source.get_prometheus_name()}_pull_duration_seconds_sum"]
        consumer_sum = metrics[f"pypipeline_{p.consumer.get_prometheus_name()}_pull_duration_seconds_sum"]

        assert toplevel_count == i + 1
        assert source_count == 2 * (i + 1)
        assert consumer_count == i + 1

        total_sleep_time = CONSUMER_SLEEP_TIME + 2 * SOURCE_SLEEP_TIME
        assert abs(toplevel_sum / toplevel_count - total_sleep_time) < 0.003
        assert abs(source_sum / source_count - SOURCE_SLEEP_TIME) < 0.003
        assert abs(consumer_sum / consumer_count - CONSUMER_SLEEP_TIME) < 0.003

    p.undeploy()
    p.delete()
