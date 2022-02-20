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

from typing import Iterable, Optional, TypeVar, Any, Dict
from fastapi import FastAPI

from pypipeline.cell import ICell, Pipeline
from pypipeline.cell.icellobserver import Event, IOCreatedEvent
from pypipeline.cellio import IO, IInput, IOutput, InternalInput, InternalOutput, IConnectionEntryPoint, \
    IConnectionExitPoint
from pypipeline.connection import Connection
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained
from pypipeline_serve.endpoints import AEndpointFactory, EncodingManager
from pypipeline_serve.endpoints.utils import get_generic_type, Decoder, Encoder


T = TypeVar("T")
EncodedType = TypeVar("EncodedType")
DecodedType = TypeVar("DecodedType")


class FastAPIServer(Pipeline):

    def __init__(self,
                 endpoint_factories: Iterable[AEndpointFactory],
                 name: str = "fastapi_server",
                 encoding_manager: EncodingManager = EncodingManager()):
        super(FastAPIServer, self).__init__(None, name=name, max_nb_internal_cells=1)
        self.__endpoint_factories = endpoint_factories
        self.__encoding_manager = encoding_manager
        self.__api_input_mapping: Dict[IInput[T], InternalInput[T]] = {}
        self.__api_output_mapping: Dict[IOutput[T], InternalOutput[T]] = {}

    def _get_internal_cell(self) -> ICell:
        internal_cells = self.get_internal_cells()
        if len(internal_cells) == 0:
            raise Exception(f"{self} has no internal cell to serve. Create an internal cell with: \n"
                            f"`server = FastAPIServer(endpoints, 'server_name')`\n"
                            f"`cell_to_serve = CellToServe(server, 'cell_name')`")
        assert len(internal_cells) == 1
        internal_cell = internal_cells[0]
        return internal_cell

    def __add_api_input_for(self, cell_input: IInput[T], api_input: InternalInput[T]) -> None:
        assert self.has_as_input(api_input)
        self.__api_input_mapping[cell_input] = api_input

    def has_api_input_for(self, cell_input: IInput) -> bool:
        return cell_input in self.__api_input_mapping

    def get_api_input_for(self, cell_input: IInput[T]) -> InternalInput[T]:
        return self.__api_input_mapping[cell_input]

    def can_have_as_io(self, io: "IO") -> "BoolExplained":
        super_result = super(FastAPIServer, self).can_have_as_io(io)
        if not super_result:
            return super_result
        if not isinstance(io, (InternalInput, InternalOutput)):
            return FalseExplained(f"{self} can only have InternalInput or InternalOutput objects as IO.")
        return TrueExplained()

    def __add_api_output_for(self, cell_output: IOutput[T], api_output: InternalOutput[T]) -> None:
        assert self.has_as_output(api_output)
        self.__api_output_mapping[cell_output] = api_output

    def has_api_output_for(self, cell_output: IOutput) -> bool:
        return cell_output in self.__api_output_mapping

    def get_api_output_for(self, cell_output: IOutput[T]) -> InternalOutput[T]:
        return self.__api_output_mapping[cell_output]

    def create_endpoints(self, app: FastAPI) -> None:
        for endpoint_factory in self.__endpoint_factories:
            endpoint_factory.create_endpoints(app, self, self.get_encoding_manager())

    # Encoding manager

    def get_encoding_manager(self) -> EncodingManager:
        return self.__encoding_manager

    def set_decoder_for_input(self, input_: IInput[DecodedType], decoder: Decoder[Any, DecodedType]) -> None:
        self.get_encoding_manager().set_decoder_for_input(input_, decoder)
        if self.has_api_input_for(input_):
            api_input = self.get_api_input_for(input_)
            self.get_encoding_manager().set_decoder_for_input(api_input, decoder)

    def get_decoder_for_input(self, input_: IInput[DecodedType]) -> Optional[Decoder[Any, DecodedType]]:
        return self.get_encoding_manager().get_decoder_for_input(input_)

    def set_encoder_for_output(self, output: IOutput[DecodedType], encoder: Encoder[DecodedType, Any]) -> None:
        self.get_encoding_manager().set_encoder_for_output(output, encoder)
        if self.has_api_output_for(output):
            api_output = self.get_api_output_for(output)
            self.get_encoding_manager().set_encoder_for_output(api_output, encoder)

    def get_encoder_for_output(self, output: IOutput[DecodedType]) -> Optional[Encoder[DecodedType, Any]]:
        return self.get_encoding_manager().get_encoder_for_output(output)

    def update(self, event: "Event") -> None:
        super(FastAPIServer, self).update(event)
        if isinstance(event, IOCreatedEvent) and event.get_initiator() == self._get_internal_cell():
            self._update_api_io()

    def _update_api_io(self) -> None:
        for original_input in self._get_internal_cell().get_inputs():
            if not self.has_api_input_for(original_input) and isinstance(original_input, IConnectionEntryPoint):
                input_name = original_input.get_name()
                input_type = get_generic_type(original_input)
                server_input = InternalInput[input_type](self, input_name)
                Connection(server_input, original_input)
                assert isinstance(original_input, IInput)   # only to make typechecking happy...
                self.__add_api_input_for(original_input, server_input)

        for original_output in self._get_internal_cell().get_outputs():
            if not self.has_api_output_for(original_output) and isinstance(original_output, IConnectionExitPoint):
                output_name = original_output.get_name()
                output_type = get_generic_type(original_output)
                server_output = InternalOutput[output_type](self, output_name)
                Connection(original_output, server_output)
                assert isinstance(original_output, IOutput)     # only to make typechecking happy...
                self.__add_api_output_for(original_output, server_output)
