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

from typing import Type, TypeVar, Generic, Callable, Any, Dict, Optional
import logging

from pypipeline.cellio import IInput, IOutput, IO


EncodedType = TypeVar("EncodedType")
DecodedType = TypeVar("DecodedType")


class ACoder(Generic[EncodedType, DecodedType]):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_encoded_type(self) -> Type:
        try:
            io_generic_type = self.__orig_class__.__args__[0]
        except AttributeError:
            # TODO raise warning
            io_generic_type = Any
        return io_generic_type

    def get_decoded_type(self) -> Type:
        try:
            io_generic_type = self.__orig_class__.__args__[1]
        except AttributeError:
            # TODO raise warning
            io_generic_type = Any
        return io_generic_type


class Decoder(ACoder[EncodedType, DecodedType], Generic[EncodedType, DecodedType]):
    """
    Decoders are used for transforming the encoded HTTP input to values that can be provided to a pipeline's inputs.

    The encoding_type parameter indicates which type should be accepted by the API (ex: str, bool, UploadFile, ...).
    The decoder parameter is a callable that converts instances of the encoding_type to the decoded type (a type
    the pipeline can accept).

    Note that the encoding_type should be supported by FastAPI: str, bool, float, datetime.datetime, UploadFile, ...
    See https://fastapi.tiangolo.com/tutorial/extra-data-types/ for more info.
    The encoding_type can also be a Pydantic model.
    """

    def __init__(self, decoder: Callable[[EncodedType], DecodedType]):
        super(Decoder, self).__init__()
        self.__decoder = decoder

    def __call__(self, item: EncodedType) -> DecodedType:
        return self.__decoder(item)


class Encoder(ACoder[EncodedType, DecodedType], Generic[DecodedType, EncodedType]):
    """
    Encoders are used for transforming pipeline outputs to encoded values that can be returned in HTTP requests.

    The encoding_type parameter indicates which type will be returned by the API (ex: str, bool, File, ...).
    The encoder parameter is a callable that converts instances of the decoded type (what the pipeline outputs) to the
    encoded type.

    Note that the encoding_type should be supported by FastAPI: str, bool, float, datetime.datetime, UploadFile, ...
    See https://fastapi.tiangolo.com/tutorial/extra-data-types/ for more info.
    The encoding_type can also be a Pydantic model.
    """

    def __init__(self, encoder: Callable[[DecodedType], EncodedType]):
        super(Encoder, self).__init__()
        self.__encoder = encoder

    def __call__(self, item: DecodedType) -> EncodedType:
        return self.__encoder(item)


class EncodingManager:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__decoders_for_inputs: Dict[IInput, Decoder] = dict()
        self.__encoders_for_outputs: Dict[IOutput, Encoder] = dict()

    def set_decoder_for_input(self, input_: IInput[DecodedType], decoder: Decoder[Any, DecodedType]) -> None:
        self.__decoders_for_inputs[input_] = decoder

    def get_decoder_for_input(self, input_: IInput[DecodedType]) -> Optional[Decoder[Any, DecodedType]]:
        return self.__decoders_for_inputs.get(input_, None)

    def set_encoder_for_output(self, output: IOutput[DecodedType], encoder: Encoder[DecodedType, Any]) -> None:
        self.__encoders_for_outputs[output] = encoder

    def get_encoder_for_output(self, output: IOutput[DecodedType]) -> Optional[Encoder[DecodedType, Any]]:
        return self.__encoders_for_outputs.get(output, None)

    def get_coder(self, io: IO[DecodedType]) -> Optional[ACoder[Any, DecodedType]]:
        if isinstance(io, IOutput):
            return self.get_encoder_for_output(io)
        elif isinstance(io, IInput):
            return self.get_decoder_for_input(io)
        assert False, f"Got unexpected type {type(io)} for {io}"

    def decode(self, value: Any, input_: IInput[DecodedType]) -> DecodedType:
        if input_ in self.__decoders_for_inputs:
            decoder = self.__decoders_for_inputs[input_]
            try:
                value = decoder(value)
            except Exception as e:
                raise Exception(f"Failed decoding value for {input_}: {value}") from e
        return value

    def encode(self, value: DecodedType, output: IOutput[DecodedType]) -> Any:
        if output in self.__encoders_for_outputs:
            encoder = self.__encoders_for_outputs[output]
            value = encoder(value)
        return value
