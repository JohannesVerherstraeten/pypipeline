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

from typing import Optional, Type
import pytest
import uvicorn
from fastapi.testclient import TestClient
from fastapi import FastAPI

from pypipeline.cell import ASingleCell, ICompositeCell, ScalableCell, Pipeline, ThreadCloneCell, RayCloneCell, \
    ICloneCell
from pypipeline.cellio import Input, Output, RuntimeParameter, ConfigParameter, InputPort, OutputPort
from pypipeline.connection import Connection
from pypipeline_serve.endpoints import ExecutionEndpointsFactory, DescriptionEndpointsFactory, \
    DeploymentEndpointsFactory, ParameterEndpointsFactory
from pypipeline_serve.fastapiserver import FastAPIServer


class ParamCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ParamCell, self).__init__(parent_cell, name=name)
        self.param: RuntimeParameter[int] = RuntimeParameter[int](self, "param")

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        param_value = self.param.pull()                      # TODO API hangs if this parameter is not pulled
        self.logger.info(f"Param value: {param_value}")


class PassThroughCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(PassThroughCell, self).__init__(parent_cell, name=name)
        self.input: Input[float] = Input[float](self, "input")
        self.output: Output[float] = Output[float](self, "output")

    def supports_scaling(self) -> bool:
        return True

    def _on_pull(self) -> None:
        value = self.input.pull()
        self.output.set_value(value)


class SomeScalableCell(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SomeScalableCell, self).__init__(parent_cell, name=name)
        self.passthrough_cell = PassThroughCell(self, "passthrough_cell")

        self.input_port_1: InputPort[float] = InputPort[float](self, "input_port_1")
        self.input_port_2: InputPort[int] = InputPort[int](self, "input_port_2")
        self.output_port_1: OutputPort[float] = OutputPort[float](self, "output_port_1")
        self.output_port_2: OutputPort[int] = OutputPort[int](self, "output_port_2")

        Connection(self.input_port_1, self.passthrough_cell.input)
        Connection(self.passthrough_cell.output, self.output_port_1)
        Connection(self.input_port_2, self.output_port_2)


class MyPipeline(Pipeline):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(MyPipeline, self).__init__(parent_cell, name=name)
        self.scalable_cell = SomeScalableCell(self, "scalable_cell")
        self.param_cell1 = ParamCell(self, "param_cell_1")
        self.param_cell2 = ParamCell(self, "param_cell_2")

        self.input_port_1: InputPort[float] = InputPort[float](self, "input_port_1")
        self.input_port_2: InputPort[int] = InputPort[int](self, "input_port_2")

        Connection(self.input_port_1, self.scalable_cell.input_port_1)
        Connection(self.input_port_2, self.scalable_cell.input_port_2)
        Connection(self.scalable_cell.output_port_1, self.param_cell1.param)
        Connection(self.scalable_cell.output_port_2, self.param_cell2.param)


class MyServer(FastAPIServer):

    def __init__(self):
        super(MyServer, self).__init__([DescriptionEndpointsFactory(),
                                        DeploymentEndpointsFactory(),
                                        ParameterEndpointsFactory(),
                                        ExecutionEndpointsFactory(input_as_form_data=True)])
        self.pipeline = MyPipeline(self, "pipeline")


app = FastAPI()
server = MyServer()
server.assert_is_valid()
server.create_endpoints(app)
client = TestClient(app)


@pytest.mark.parametrize("scaleup_method", [ThreadCloneCell, RayCloneCell])
def test_1(scaleup_method: Type[ICloneCell]):
    server.pipeline.scalable_cell.scale_up(method=scaleup_method)

    # Set default param
    response = client.post("/parameters", json={
        "pipeline": {
            "param_cell_1": {
                "param": 1
            },
            "param_cell_2": {
                "param": 2
            }
        }
    })
    assert response.status_code == 200

    # Deploy
    response = client.post("/deployment", json={})
    assert response.status_code == 200, response.json()

    # Pull
    files = {
        'input_port_1': (None, "88".encode()),
        # 'input_port_2': (None, "99".encode())
    }
    response = client.post("/pull", files=files)
    assert response.status_code == 200, response.json()

    # Pull
    files = {
        'input_port_1': (None, "88".encode()),
        'input_port_2': (None, "99".encode())
    }
    response = client.post("/pull", files=files)
    assert response.status_code == 200, response.json()

    # Pull
    files = {
        'input_port_1': (None, "88".encode()),
        # 'input_port_2': (None, "99".encode())
    }
    response = client.post("/pull", files=files)
    assert response.status_code == 200, response.json()
