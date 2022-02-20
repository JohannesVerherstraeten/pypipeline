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
import time

from pypipeline.cell import ASingleCell, ICompositeCell, ScalableCell, Pipeline, ThreadCloneCell, RayCloneCell, \
    ICloneCell
from pypipeline.cellio import Input, Output, RuntimeParameter, ConfigParameter, InputPort, OutputPort
from pypipeline.connection import Connection
from pypipeline_serve.endpoints import ExecutionEndpointsFactory, DescriptionEndpointsFactory, \
    DeploymentEndpointsFactory, ParameterEndpointsFactory
from pypipeline_serve.fastapiserver import FastAPIServer


class SlowCell(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SlowCell, self).__init__(parent_cell, name=name)
        self.input1: Input[float] = Input[float](self, "input1")
        self.input2: Input[int] = Input[int](self, "input2")
        self.output: Output[float] = Output[float](self, "output")

    def supports_scaling(self) -> bool:
        return True

    def _on_pull(self) -> None:
        value1 = self.input1.pull()
        value2 = self.input2.pull()
        time.sleep(1)
        result = value1 + value2
        self.output.set_value(result)


class SomeScalableCell(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(SomeScalableCell, self).__init__(parent_cell, name=name)
        self.slow_cell = SlowCell(self, "slow_cell")

        self.input_port_1: InputPort[float] = InputPort[float](self, "input_port_1")
        self.input_port_2: InputPort[int] = InputPort[int](self, "input_port_2")
        self.output_port_1: OutputPort[float] = OutputPort[float](self, "output_port_1")

        Connection(self.input_port_1, self.slow_cell.input1)
        Connection(self.input_port_2, self.slow_cell.input2)
        Connection(self.slow_cell.output, self.output_port_1)


class MyServer(FastAPIServer):

    def __init__(self):
        super(MyServer, self).__init__([DescriptionEndpointsFactory(),
                                        DeploymentEndpointsFactory(),
                                        ParameterEndpointsFactory(),
                                        ExecutionEndpointsFactory(input_as_form_data=True)])
        self.scalable_cell = SomeScalableCell(self, "scalable_cell")


app = FastAPI()
server = MyServer()
server.scalable_cell.scale_up(2, method=ThreadCloneCell)
server.assert_is_valid()
server.create_endpoints(app)
client = TestClient(app)
# uvicorn.run(app)


from httpx import AsyncClient
import asyncio
import logging


async def do_pull_call(ac: AsyncClient, input_value_1: float, input_value_2: int):
    logging.warning(f"{time.time()}: call started")
    files = {
        'input_port_1': (None, f"{input_value_1}".encode()),
        'input_port_2': (None, f"{input_value_2}".encode())
    }
    response = await ac.post("/pull", files=files)
    assert response.status_code == 200
    assert response.json()["output_port_1"] == input_value_1 + input_value_2
    return response


async def scale_down_after(seconds: float):
    await asyncio.sleep(seconds)
    server.scalable_cell.scale_one_down(method=ThreadCloneCell)
    return


# TODO this test with live downscaling doesn't succeed yet
# @pytest.mark.asyncio
# async def test_pull_asyncio():
#     # Deploy
#     response = client.post("/deployment", json={})
#     assert response.status_code == 200, response.json()
#
#     t0 = time.time()
#     async with AsyncClient(app=app, base_url="http://test") as ac:
#         coroutines = [scale_down_after(1)]
#         for i in range(6):
#             coroutines.append(do_pull_call(ac, i + i/10, i))
#         t1 = time.time()
#
#         t2 = time.time()
#         results = await asyncio.gather(*coroutines)
#     t3 = time.time()
#     logging.info(f"Submission time: {t1-t0}, Gather time: {t3-t2}")
