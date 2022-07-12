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

from typing import Optional, TYPE_CHECKING, Generator, Dict
import ray
import numpy as np
from math import sin, pi
import pytest
from tqdm import tqdm
from prometheus_client import start_http_server, CollectorRegistry
import requests

from pypipeline.cell import ASingleCell, ScalableCell, RayCloneCell, ThreadCloneCell, Pipeline
from pypipeline.cellio import Input, Output, InputPort, OutputPort
from pypipeline.connection import Connection

if TYPE_CHECKING:
    from pypipeline.cell import ICompositeCell


NB_ITERATIONS = 1000
SQUARE_MATRIX_SIZE = 3


@pytest.fixture
def ray_init_and_shutdown() -> Generator:
    ray.init()
    yield None
    ray.shutdown()


class SourceCell1(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SourceCell1, self).__init__(parent_cell, name=name)
        self.output_counter: Output[int] = Output(self, "counter")
        self.output_sine: Output[float] = Output(self, "sine")
        self.counter = 1

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        self.output_counter.set_value(self.counter)
        self.output_sine.set_value(sin(self.counter * pi/2))
        # self.logger.debug(f" ! Outputs: {self.counter}, {sin(self.counter * pi/2)}")
        self.counter += 1


class SourceCell2(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SourceCell2, self).__init__(parent_cell, name=name)
        self.output_matrix: Output[np.ndarray] = Output(self, "matrix")
        self.current_diagonal: np.ndarray = np.ones((SQUARE_MATRIX_SIZE, ))

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        result = np.random.random((SQUARE_MATRIX_SIZE, SQUARE_MATRIX_SIZE))
        result[np.where(np.eye(SQUARE_MATRIX_SIZE) > 0)] = self.current_diagonal
        self.output_matrix.set_value(result)
        # self.logger.debug(f" ! Outputs: {self.current_diagonal}")
        self.current_diagonal = self.current_diagonal + 1


class CenterCell1(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(CenterCell1, self).__init__(parent_cell, name=name)
        self.input_counter: Input[int] = Input(self, "counter")
        self.input_sine: Input[float] = Input(self, "sine")
        self.output_multiplication: Output[float] = Output(self, "multiplication")

    def supports_scaling(self) -> bool:
        return True

    def _on_pull(self) -> None:
        counter = self.input_counter.pull()
        sine = self.input_sine.pull()
        multiple = counter * sine
        self.output_multiplication.set_value(multiple)
        # self.logger.debug(f" ! Inputs: {counter}, {sine}, Outputs: {multiple}")


class ScalableCell1(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ScalableCell1, self).__init__(parent_cell, name=name)

        self.input_counter: InputPort[int] = InputPort(self, "counter_in")
        self.input_sine: InputPort[float] = InputPort(self, "sine")
        self.output_multiplication: OutputPort[float] = OutputPort(self, "multiplication")
        self.output_counter: OutputPort[int] = OutputPort(self, "counter_out")

        self.center_cell_1 = CenterCell1(self, "center_cell_1")

        Connection(self.input_counter, self.center_cell_1.input_counter)
        Connection(self.input_counter, self.output_counter)
        Connection(self.input_sine, self.center_cell_1.input_sine)
        Connection(self.center_cell_1.output_multiplication, self.output_multiplication)


class CenterCell2(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(CenterCell2, self).__init__(parent_cell, name=name)
        self.input_matrix: Input[np.ndarray] = Input(self, "matrix")
        self.input_sine: Input[float] = Input(self, "sine")
        self.output_matrix_multiple: Output[np.ndarray] = Output(self, "matrix_multiple")

    def supports_scaling(self) -> bool:
        return True

    def _on_pull(self) -> None:
        sine = self.input_sine.pull()
        matrix = self.input_matrix.pull()
        matrix_multiple: np.ndarray = matrix * sine
        self.output_matrix_multiple.set_value(matrix_multiple)
        # self.logger.debug(f" ! Inputs: {matrix}, {sine}, Outputs: {matrix_multiple}")


class ScalableCell2(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ScalableCell2, self).__init__(parent_cell, name=name)

        self.input_matrix: InputPort[np.ndarray] = InputPort(self, "matrix")
        self.input_sine: InputPort[float] = InputPort(self, "sine")
        self.output_matrix_multiplication: OutputPort[np.ndarray] = OutputPort(self, "matrix_multiplication")

        self.center_cell_2 = CenterCell2(self, "center_cell_2")

        Connection(self.input_matrix, self.center_cell_2.input_matrix)
        Connection(self.input_sine, self.center_cell_2.input_sine)
        Connection(self.center_cell_2.output_matrix_multiple, self.output_matrix_multiplication)


class SinkCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SinkCell, self).__init__(parent_cell, name=name)
        self.input_multiplication: Input[int] = Input(self, "multiplication")
        self.input_counter: Input[int] = Input(self, "counter")
        self.input_matrix_multiple: Input[np.ndarray] = Input(self, "matrix_multiple")
        self.output_combination: Output[np.ndarray] = Output(self, "combination")

    def supports_scaling(self) -> bool:
        return True

    def _on_pull(self) -> None:
        multiplication = self.input_multiplication.pull()
        counter = self.input_counter.pull()
        matrix_multiple = self.input_matrix_multiple.pull()
        combination: np.ndarray = (matrix_multiple + np.eye(matrix_multiple.shape[0]) * multiplication) / 2.
        combination /= counter      # Should be just the sine signal again on the diagonal
        self.output_combination.set_value(combination)
        # self.logger.debug(f" ! Inputs: {multiplication}, {counter}, {matrix_multiple}, Outputs: {combination}")


class SinkScalableCell(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SinkScalableCell, self).__init__(parent_cell, name=name)

        self.input_multiplication: InputPort[int] = InputPort(self, "multiplication")
        self.input_counter: InputPort[int] = InputPort(self, "counter")
        self.input_matrix_multiple: InputPort[np.ndarray] = InputPort(self, "matrix_multiple")
        self.output_combination: OutputPort[np.ndarray] = OutputPort(self, "combination")

        self.center_cell_3 = SinkCell(self, "center_cell_3")

        Connection(self.input_multiplication, self.center_cell_3.input_multiplication)
        Connection(self.input_counter, self.center_cell_3.input_counter)
        Connection(self.input_matrix_multiple, self.center_cell_3.input_matrix_multiple)
        Connection(self.center_cell_3.output_combination, self.output_combination)


class ToplevelPipeline(Pipeline):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ToplevelPipeline, self).__init__(parent_cell, name=name)

        self.source_1 = SourceCell1(self, "source_1")
        self.source_2 = SourceCell2(self, "source_2")
        self.center_1 = ScalableCell1(self, "center_1_scalable")
        self.center_2 = ScalableCell2(self, "center_2_scalable")
        self.sink = SinkScalableCell(self, "sink_scalable")

        Connection(self.source_1.output_counter, self.center_1.input_counter)
        Connection(self.source_1.output_sine, self.center_1.input_sine)
        Connection(self.source_1.output_sine, self.center_2.input_sine)
        Connection(self.source_2.output_matrix, self.center_2.input_matrix)
        Connection(self.center_1.output_counter, self.sink.input_counter)
        Connection(self.center_1.output_multiplication, self.sink.input_multiplication)
        Connection(self.center_2.output_matrix_multiplication, self.sink.input_matrix_multiple)


@pytest.fixture()
def toplevel_pipeline() -> ToplevelPipeline:
    return ToplevelPipeline(None, "toplevel")


def test_1(ray_init_and_shutdown: None, toplevel_pipeline: ToplevelPipeline) -> None:
    toplevel_pipeline.center_1.scale_up(3, ThreadCloneCell)
    toplevel_pipeline.center_2.scale_up(2, RayCloneCell)
    toplevel_pipeline.sink.scale_up(4, RayCloneCell)

    toplevel_pipeline.assert_is_valid()
    toplevel_pipeline.deploy()
    toplevel_pipeline.assert_is_valid()

    PORT = 8808
    start_http_server(PORT, registry=toplevel_pipeline.get_prometheus_metric_registry())

    for i in tqdm(range(1, NB_ITERATIONS+1)):
        expected_result: np.ndarray = np.ones((SQUARE_MATRIX_SIZE, )) * sin(i * pi/2)

        toplevel_pipeline.pull()
        actual_result = toplevel_pipeline.sink.output_combination.get_value()

        toplevel_pipeline.logger.info(f"Expected result: {expected_result}")
        toplevel_pipeline.logger.info(f"Actual result: {actual_result.diagonal()}")

        assert np.isclose(actual_result.diagonal(), expected_result).all()

        if i % 10 == 0:
            response = requests.get(f"http://localhost:{PORT}/metrics")
            assert response.status_code == 200

    response = requests.get(f"http://localhost:{PORT}/metrics")
    assert response.status_code == 200

    print(response.text)

    assert_correct_prometheus_metrics(toplevel_pipeline, response.text, NB_ITERATIONS)

    toplevel_pipeline.undeploy()
    toplevel_pipeline.delete()


def assert_correct_prometheus_metrics(pipeline: ToplevelPipeline, metrics_text: str, nb_iterations: int):
    metrics: Dict[str, float] = dict()
    for line in metrics_text.splitlines():
        if line.startswith(f"pypipeline_"):
            metric_name, metric_value = line.split(" ")
            metrics[metric_name] = float(metric_value)
    assert metrics[f"pypipeline_{pipeline.get_prometheus_name()}_pull_duration_seconds_count"] == nb_iterations
    assert_correct_scalable_cell_prometheus_metrics(pipeline.center_1, metrics=metrics, nb_iterations=nb_iterations)
    assert_correct_scalable_cell_prometheus_metrics(pipeline.center_2, metrics=metrics, nb_iterations=nb_iterations)


def assert_correct_scalable_cell_prometheus_metrics(scalable_cell: ScalableCell,
                                                    metrics: Dict[str, float],
                                                    nb_iterations: int):
    prometheus_name = scalable_cell.get_prometheus_name()
    total_pulls = 0
    for metric_name, metric_value in metrics.items():
        if metric_name.startswith(f"pypipeline_{prometheus_name}_pull_duration_seconds_count"):
            total_pulls += metric_value
    assert total_pulls >= nb_iterations
