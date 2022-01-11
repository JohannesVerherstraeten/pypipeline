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

# TODO provide some explanation

from typing import Optional
import uvicorn
import time
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
        # self.param1.set_value(np.array([1, 2, 3]))
        self.param2: RuntimeParameter[np.ndarray] = RuntimeParameter[np.ndarray](self, "param2")
        # self.param2.set_value(np.array([4, 5, 6]))
        self.output: Output[float] = Output(self, "output")

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        value1: float = self.input1.pull()  # typechecks!
        value2: np.ndarray = self.input2.pull()
        self.param1.pull()
        self.param2.pull()
        result_sum = value1 + np.sum(value2)
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

        self.set_decoder_for_input(self.cell_b.input2, image_decoder)
        self.set_decoder_for_input(self.cell_b.param1, lst_decoder)
        self.set_decoder_for_input(self.cell_b.param2, lst_decoder)




app1 = FastAPI()
server1 = CellBServer()
server1.create_endpoints(app1)
# uvicorn.run(app1, host="0.0.0.0", port=8808)


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

        self.set_decoder_for_input(self.scalable_cell_b.input_port_2, image_decoder)
        self.set_decoder_for_input(self.scalable_cell_b.param_port, lst_decoder)
        self.set_decoder_for_input(self.scalable_cell_b.cell_b.param1, lst_decoder)
        self.set_decoder_for_input(self.scalable_cell_b.cell_b.param2, lst_decoder)


app2 = FastAPI()
server2 = ScalableCellBServer()
server2.scalable_cell_b.scale_up(CELL_B_SCALE_UP)       # TODO use RuntimeParameter for this
server2.create_endpoints(app2)
uvicorn.run(app2, host="0.0.0.0", port=8809)

