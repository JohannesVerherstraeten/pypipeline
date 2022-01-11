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

from typing import Optional
import uvicorn
import time
from fastapi.testclient import TestClient
import numpy as np
from fastapi import FastAPI, UploadFile
from PIL import Image

from pypipeline.cell import ASingleCell, ICompositeCell, ScalableCell
from pypipeline.cellio import Input, Output, RuntimeParameter, ConfigParameter, InputPort, OutputPort
from pypipeline.connection import Connection
from pypipeline_serve.endpoints import ExecutionEndpointsFactory, DescriptionEndpointsFactory, \
    DeploymentEndpointsFactory, ParameterEndpointsFactory, Decoder
from pypipeline_serve.fastapiserver import FastAPIServer


CELL_B_SLEEP_TIME = 2
CELL_B_SCALE_UP = 3


class CellB(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(CellB, self).__init__(parent_cell, name=name)
        self.input1: Input[float] = Input[float](self, "input1")
        self.input2: Input[np.ndarray] = Input[np.ndarray](self, "input2",
                                                           validation_fn=lambda x: isinstance(x, np.ndarray))
        self.config1: ConfigParameter[str] = ConfigParameter[str](self, "config1")
        # self.config1.set_value("myconfig")
        self.param1: RuntimeParameter[np.ndarray] = RuntimeParameter[np.ndarray](self, "param1")
        # TODO include these parameters in the POST /deploy params?
        #  -> Otherwise they need to be set with POST /params before deploying is possible
        # self.param1.set_value(np.array([1, 2, 3]))
        self.param2: RuntimeParameter[np.ndarray] = RuntimeParameter[np.ndarray](self, "param2")
        # self.param2.set_value(np.array([4, 5, 6]))
        self.output: Output[float] = Output(self, "output")

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        value1: float = self.input1.pull()  # typechecks!
        value2: np.ndarray = self.input2.pull()
        self.param1.pull()                      # TODO API hangs if this parameter is not pulled
        self.param2.pull()
        result_sum = value1 + np.sum(value2)  # TODO what if returning a numpy array to the api without encoder?
        time.sleep(CELL_B_SLEEP_TIME)
        # print(f"[CellB]: log: {result_sum}")
        self.logger.debug(f"setting value: {result_sum}")
        self.output.set_value(result_sum)


def decode_array_from_img(x: UploadFile) -> np.ndarray:
    print(f"Decoding array")
    return np.array(Image.open(x.file))


def decode_array_from_str(x: str) -> np.ndarray:
    print(f"Decoding array")
    list_ints = [int(number) for number in x.split(",")]
    return np.array(list_ints)


# The first typing is needed for mypy checking, the second one is needed for FastAPI docs generation...
# Not pretty, but this is how python handles typing...
image_decoder: Decoder[UploadFile, np.ndarray] = Decoder[UploadFile, np.ndarray](decode_array_from_img)
lst_decoder: Decoder[str, np.ndarray] = Decoder[str, np.ndarray](decode_array_from_str)


class CellBServer(FastAPIServer):

    def __init__(self):
        super(CellBServer, self).__init__([DescriptionEndpointsFactory(),
                                           DeploymentEndpointsFactory(),
                                           ParameterEndpointsFactory(),
                                           ExecutionEndpointsFactory(input_as_form_data=True)])
        self.cell_b = CellB(self, "cell_b")

        # TODO improve error message when one of these lines is missing...
        self.set_decoder_for_input(self.cell_b.input2, image_decoder)
        self.set_decoder_for_input(self.cell_b.param1, lst_decoder)
        self.set_decoder_for_input(self.cell_b.param2, lst_decoder)




app1 = FastAPI()
server1 = CellBServer()
server1.create_endpoints(app1)
# uvicorn.run(app1, host="0.0.0.0", port=8808)
client1 = TestClient(app1)


# Scalable cell B with async support:

class ScalableCellB(ScalableCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(ScalableCellB, self).__init__(parent_cell, name=name)
        self.cell_b = CellB(self, "cell_b")

        self.input_port_1: InputPort[float] = InputPort[float](self, "input_port_1")
        self.input_port_2: InputPort[np.ndarray] = InputPort[np.ndarray](self, "input_port_2")
        self.param_port: InputPort[np.ndarray] = InputPort[np.ndarray](self, "param_port")
        self.output_port: OutputPort[float] = OutputPort[float](self, "output_port")

        Connection(self.input_port_1, self.cell_b.input1)
        Connection(self.input_port_2, self.cell_b.input2)
        Connection(self.param_port, self.cell_b.param1)
        Connection(self.cell_b.output, self.output_port)


class ScalableCellBServer(FastAPIServer):

    def __init__(self):
        super(ScalableCellBServer, self).__init__([DescriptionEndpointsFactory(),
                                                   DeploymentEndpointsFactory(),
                                                   ParameterEndpointsFactory(),
                                                   ExecutionEndpointsFactory(input_as_form_data=True)])
        self.scalable_cell_b = ScalableCellB(self, "scalable_cell_b")

        # TODO improve error message when one of these lines is missing...
        self.set_decoder_for_input(self.scalable_cell_b.input_port_2, image_decoder)
        self.set_decoder_for_input(self.scalable_cell_b.param_port, lst_decoder)
        self.set_decoder_for_input(self.scalable_cell_b.cell_b.param1, lst_decoder)
        self.set_decoder_for_input(self.scalable_cell_b.cell_b.param2, lst_decoder)


app2 = FastAPI()
server2 = ScalableCellBServer()
server2.scalable_cell_b.scale_up(CELL_B_SCALE_UP)       # TODO make endpoint for this? Or use RuntimeParam?
server2.create_endpoints(app2)
# uvicorn.run(app2, host="0.0.0.0", port=8809)
client2 = TestClient(app2)


import pytest
from httpx import AsyncClient
from io import BytesIO
import asyncio
from math import ceil
import logging


@pytest.mark.parametrize("client, server", [(client1, server1), (client2, server2)])
def test_get_deployment(client: TestClient, server: FastAPIServer):
    response = client.get("/deployment")
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["pipeline_name"] == server.get_name()
    assert response_json["is_deployed"] == server.is_deployed()


@pytest.mark.parametrize("client, server", [(client1, server1), (client2, server2)])
def test_post_deployment_without_default_parameters(client: TestClient, server: FastAPIServer):
    response = client.post("/deployment", json={})
    assert response.status_code == 400, response.json()
    assert not server.is_deployed()


@pytest.mark.parametrize("client, server", [(client1, server1)])
def test_post_runtime_parameters_1(client: TestClient, server: FastAPIServer):
    response = client.post("/parameters", json={
        "cell_b": {
            "param1": "1,2,3",
            "param2": "4,5,6"
        }
    })
    assert response.status_code == 200


@pytest.mark.parametrize("client, server", [(client2, server2)])
def test_post_runtime_parameters_2(client: TestClient, server: FastAPIServer):
    response = client.post("/parameters", json={
        "scalable_cell_b": {
            "cell_b": {
                "param1": "1,2,3",
                "param2": "4,5,6"
            }
        }
    })
    assert response.status_code == 200


@pytest.mark.parametrize("client, server", [(client1, server1)])
def test_post_deployment_1(client: TestClient, server: FastAPIServer):
    response = client.post("/deployment", json={
        "cell_b": {
            "config1": "some_config"
        }
    })
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json["is_deployed"]
    assert server.is_deployed()


@pytest.mark.parametrize("client, server", [(client2, server2)])
def test_post_deployment_2(client: TestClient, server: FastAPIServer):
    response = client.post("/deployment", json={
        "scalable_cell_b": {
            "queue_capacity": 3,
            "check_quit_interval": 10,
            "cell_b": {
                "config1": "some_config"
            }
        }
    })
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["is_deployed"]
    assert server.is_deployed()


@pytest.mark.parametrize("client, server", [(client1, server1), (client2, server2)])
def test_delete_deployment(client: TestClient, server: FastAPIServer):
    response = client.delete("/deployment")
    assert response.status_code == 200
    response_json = response.json()
    assert not response_json["is_deployed"]


@pytest.fixture
def dummy_img_data() -> bytes:
    dummy_img = Image.new(mode="RGB", size=(10, 10), color=(255, 0, 0))
    dummy_img_bytesio = BytesIO()
    dummy_img.save(dummy_img_bytesio, format="png")
    dummy_img_bytesio.seek(0)
    dummy_img_bytes = dummy_img_bytesio.read()
    return dummy_img_bytes


@pytest.mark.parametrize("client, server", [(client1, server1)])
def test_pull_syncio_1(dummy_img_data, client: TestClient, server: FastAPIServer):
    response = client.post("/deployment", json={
        "cell_b": {
            "config1": "some_config"
        }
    })
    assert response.status_code == 200
    response = client.post("/parameters", json={
        "cell_b": {
            "param1": "1,2,3",
            "param2": "4,5,6"
        }
    })
    assert response.status_code == 200
    files = {
        'input1': (None, "88".encode()),
        'input2': ("random_test_file.png", dummy_img_data),    # Filename is required!
        "param1": (None, "1,2,3".encode()),      # TODO if these are not here, it hangs
        "param2": (None, "4,5,6".encode()),      # TODO is weird formatting needed?
    }
    response = client.post("/pull", files=files)
    assert response.status_code == 200, response.json()
    assert response.json()["output"] == 25588


@pytest.mark.parametrize("client, server", [(client2, server2)])
def test_pull_syncio_2(dummy_img_data, client: TestClient, server: FastAPIServer):
    response = client.post("/deployment", json={
        "scalable_cell_b": {
            "queue_capacity": 3,
            "check_quit_interval": 10,
            "cell_b": {
                "config1": "some_config"
            }
        }
    })
    assert response.status_code == 200
    response = client.post("/parameters", json={
        "scalable_cell_b": {
            "cell_b": {
                "param1": "1,2,3",
                "param2": "4,5,6"
            }
        }
    })
    assert response.status_code == 200
    files = {
        'input_port_1': (None, "88".encode()),
        'param_port': (None, "7,8,9".encode()),
        'input_port_2': ("random_test_file.png", dummy_img_data),    # Filename is required!
    }
    response = client.post("/pull", files=files)
    assert response.status_code == 200, response.json()
    assert response.json()["output_port"] == 25588


async def do_pull_call(ac: AsyncClient, files):
    logging.warning(f"{time.time()}: call started")
    response = await ac.post("/pull", files=files)
    return response


@pytest.mark.asyncio
async def test_pull_asyncio(dummy_img_data):
    files_1 = {
        'input_port_1': (None, "88".encode()),
        'param_port': (None, "7,8,9".encode()),
        'input_port_2': ("random_test_file.png", dummy_img_data),    # Filename is required!
    }
    files_2 = {
        'input_port_1': (None, "89".encode()),
        'param_port': (None, "7,8,9".encode()),
        'input_port_2': ("random_test_file.png", dummy_img_data),    # Filename is required!
    }
    files_3 = {
        'input_port_1': (None, "90".encode()),
        'param_port': (None, "7,8,9".encode()),
        'input_port_2': ("random_test_file.png", dummy_img_data),    # Filename is required!
    }
    files_4 = {
        'input_port_1': (None, "91".encode()),
        'param_port': (None, "7,8,9".encode()),
        'input_port_2': ("random_test_file.png", dummy_img_data),    # Filename is required!
    }
    t0 = time.time()
    async with AsyncClient(app=app2, base_url="http://test") as ac:
        coroutines = [do_pull_call(ac, files_1),
                      do_pull_call(ac, files_2),
                      do_pull_call(ac, files_3),
                      do_pull_call(ac, files_4)]
        expected_results = [25588, 25589, 25590, 25591]

        results = await asyncio.gather(*coroutines)
    for response, expected_result in zip(results, expected_results):
        assert response.status_code == 200, response.json()
        assert response.json()["output_port"] == expected_result
    t1 = time.time()
    elapsed = t1 - t0
    print(f"Elapsed: {elapsed}")
    # Keep a margin of 1.2 for overhead of logging etc...
    assert elapsed < ceil(len(expected_results) / CELL_B_SCALE_UP) * CELL_B_SLEEP_TIME * 1.2, f"Elapsed: {elapsed}"

# TODO test error occurs during encoding or decoding
