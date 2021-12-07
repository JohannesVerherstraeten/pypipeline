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

import inspect
from typing import Type, Any, Dict, Tuple, Iterable, Optional, Union, TypeVar, List
from fastapi import Form, File, UploadFile
from pydantic import BaseModel, create_model, Field
import logging

from pypipeline.cellio import IO, IInput, ConfigParameter, RuntimeParameter, InternalInput, InternalOutput
from pypipeline.cell import ICompositeCell, ICell
from pypipeline.exceptions import UnconnectedException, NoInputProvidedException
from pypipeline_serve.endpoints.utils.encoding import EncodingManager


T = TypeVar("T")


def __get_orig_class(obj):
    """
    Function copied from https://github.com/Stewori/pytypes/blob/master/pytypes/type_util.py
    -> Not using the library itself as it has no official python3.7/3.8 support ATM. TODO

    Robust way to access `obj.__orig_class__`. Compared to a direct access this has the
    following advantages:
    1) It works around https://github.com/python/typing/issues/658.
    2) It prevents infinite recursion when wrapping a method (`obj` is `self` or `cls`) and either
       - the object's class defines `__getattribute__`
       or
       - the object has no `__orig_class__` attribute and the object's class defines `__getattr__`.
       See discussion at https://github.com/Stewori/pytypes/pull/53.
    `AttributeError` is raised in failure case.
    """
    try:
        return object.__getattribute__(obj, '__orig_class__')
    except AttributeError:
        cls = object.__getattribute__(obj, '__class__')
        # Workaround for https://github.com/python/typing/issues/658
        stck = inspect.stack()
        # Searching from index 2 is sufficient: At 0 is get_orig_class, at 1 is the caller.
        # We assume the caller is not typing._GenericAlias.__call__ which we are after.
        for line in stck[2:]:
            try:
                res = line[0].f_locals['self']
                if res.__origin__ is cls:
                    return res
            except (KeyError, AttributeError):
                pass
        raise


def get_generic_type(io: IO[T]) -> Type[T]:
    """
    Extracts the typing information from the IO object.

    See following StackOverflow:
    https://stackoverflow.com/questions/57706180/generict-base-class-how-to-get-type-of-t-from-within-instance
    """
    try:
        io_generic_type = __get_orig_class(io).__args__[0]
        # io_generic_type = io.__orig_class__.__args__[0]
    except AttributeError:
        logging.warning(f"Cannot extract typing info from {io}, "
                        f"so the API docs won't show relevant example values or schema typing info for this input. "
                        f"\n\tTo improve your API documentation, please add a type hint to the right-hand side of your "
                        f"cell IO declarations as well: "
                        f"`{io.get_name()}: {type(io).__name__}[<Type>] = "
                        f"{type(io).__name__}[<Type>](self, '{io.get_name()}')` "
                        f"-> note the second type hint.")
        io_generic_type = Any

    return io_generic_type


def create_pydantic_field(cell_io: IO,
                          encoding_manager: Optional[EncodingManager] = None) -> Tuple[str, Type, Field]:
    io_type = get_generic_type(cell_io)

    if encoding_manager is not None:
        coder = encoding_manager.get_coder(cell_io)
        field_type = coder.get_encoded_type() if coder is not None else io_type
    else:
        field_type = io_type

    io_default_value = cell_io.get_value() if cell_io.value_is_set() else ...
    if isinstance(cell_io, IInput) and cell_io._is_optional_even_when_typing_says_otherwise():
        field_type = Optional[field_type]
    field_name = cell_io.get_name()
    return field_name, field_type, Field()


def create_pydantic_model_recursively(model_name: str,
                                      cell_: "ICell",
                                      io_objects: Iterable[IO],
                                      encoding_manager: Optional[EncodingManager] = None,
                                      full_model_required: bool = True) -> Optional[Type[BaseModel]]:
    """
    For docs, see the similar function create_pydantic_model() below.
    """
    # The model dict representing the pydantic model:
    # Dictionary with field names, mapping to a tuple with the type of the field and a default_value or Field object
    model_dict: Dict[str, Tuple[Type, Any]] = dict()

    # Add the IO from this cell
    all_cell_io: List[IO] = list(cell_.get_inputs())
    all_cell_io += list(cell_.get_outputs())
    for cell_io in all_cell_io:
        if cell_io not in io_objects:
            continue

        field_name, field_type, field = create_pydantic_field(cell_io, encoding_manager)
        model_dict[field_name] = (field_type, field)  # (type, default_value or Field object)

    # Add the IO from internal cells
    if isinstance(cell_, ICompositeCell):
        for internal_cell in cell_.get_internal_cells():
            InternalCellConfigModelIn = create_pydantic_model_recursively(f"{model_name}_{internal_cell.get_name()}",
                                                                          internal_cell,
                                                                          io_objects,
                                                                          encoding_manager,
                                                                          full_model_required)
            if InternalCellConfigModelIn is not None:
                if not full_model_required:
                    InternalCellConfigModelIn = Optional[InternalCellConfigModelIn]
                model_dict[internal_cell.get_name()] = (InternalCellConfigModelIn, Field())

    # If no relevant IO was found to make a model from, return None
    if len(model_dict) == 0:
        return None

    # Otherwise, build the model
    try:
        model_class: Type[BaseModel] = create_model(model_name, **model_dict)
    except RuntimeError as e:
        logging.error(f"Cannot create the Pydantic model `{model_name}`. It probably contains a type that is "
                      f"not supported by FastAPI. See https://fastapi.tiangolo.com/tutorial/extra-data-types/ for "
                      f"supported types, or use `File` or `UploadFile` for binary data. ")
        raise e
    return model_class


def create_pydantic_model(model_name: str,
                          io_objects: Iterable[IO],
                          encoding_manager: Optional[EncodingManager] = None) -> Type[BaseModel]:
    """
    Dynamically creates a Pydandic model type with the given information.

    The returned type is equivalent to the following:

    class model_name(BaseModel):
        io_name_1: io_type_1 = Field(default=io_value_1)        # io_type_1 is the generic type of io_object_1.
        io_name_2: io_type_2 = Field(default=io_value_2)
        io_name_3: encoded_type_3 = Field(default=io_value_3_encoded)   # if a decoder is defined for io_object_3
        ...

    TODO: Field(..., description=<descr>, example=<value>) is possible too, use it?

    The decoders are used for transforming the HTTP input to values that can be provided to the pipeline's inputs.
    """
    # The model dict representing the pydantic model:
    # Dictionary with field names, mapping to a tuple with the type of the field and a default_value or Field object
    model_dict: Dict[str, Tuple[Type, Any]] = dict()
    logging.debug(f"Model generation for {model_name}:")
    for io in io_objects:
        field_name, field_type, field = create_pydantic_field(io, encoding_manager)
        print(f"  {field_name}: ({field_type}, {Field()})")
        model_dict[field_name] = (field_type, Field())  # (type, default_value or Field object)
    try:
        model_class: Type[BaseModel] = create_model(model_name, **model_dict)
    except RuntimeError as e:
        logging.error(f"Cannot create the Pydantic model `{model_name}`. It probably contains a type that is "
                      f"not supported by FastAPI. See https://fastapi.tiangolo.com/tutorial/extra-data-types/ for "
                      f"supported types, or use `File` or `UploadFile` for binary data. ")
        raise e
    return model_class


def decorate_as_form(cls: Type[BaseModel]) -> Type[BaseModel]:
    """
    Based on https://github.com/tiangolo/fastapi/issues/2387

    Change: the field default should be a File(...) instead of a Form(...) in case the user wants to
    include a UploadFile/File object in the form.

    Adds an as_form class method to decorated models. The as_form class method
    can be used with FastAPI endpoints
    """
    new_params = []
    for field in cls.__fields__.values():
        is_file_field: bool = field.outer_type_ == File or field.outer_type_ == UploadFile
        FileOrForm: Union[Type[File], Type[Form]] = File if is_file_field else Form
        default: Union[File, Form] = (FileOrForm(field.default) if not field.required else FileOrForm(...))
        new_param = inspect.Parameter(field.alias,
                                      inspect.Parameter.POSITIONAL_ONLY,
                                      default=default,
                                      annotation=field.outer_type_)
        new_params.append(new_param)

    async def _as_form(**data):
        return cls(**data)

    sig = inspect.signature(_as_form)
    sig = sig.replace(parameters=new_params)
    _as_form.__signature__ = sig
    setattr(cls, "as_form", _as_form)
    return cls


def set_values(cell: "ICell", value_mapping: Optional[BaseModel], encoding_manager: Optional[EncodingManager] = None):
    # Why not validate all values before setting the inputs to avoid that first ones might already be
    # used while one of the later ones raises an exception... -> Doesn't solve anything, as the values can only
    # be validated further up in the pipeline, therefore they need to be pulled before validation can happen.
    logging.info(f"Set inputs/parameters of {cell} with {value_mapping}")
    if value_mapping is None:       # In case of non required parameters, (parts of) the pydantic model may be None
        return
    try:
        for key, value in value_mapping:
            if isinstance(cell, ICompositeCell) and cell.has_as_internal_cell_name(key):
                internal_cell = cell.get_internal_cell_with_name(key)    # TODO may raise exceptions
                set_values(internal_cell, value, encoding_manager)
                continue
            io = cell.get_input(key)    # TODO input validation     # TODO may raise exceptions
            assert isinstance(io, (RuntimeParameter, ConfigParameter, InternalInput)), f"Got {io}"    # TODO generalize interface?
            if value is not None:
                decoded_value = encoding_manager.decode(value, io) if encoding_manager is not None else value
                io.set_value(decoded_value)
            elif isinstance(io, InternalInput):
                io.set_value(None)      # TODO this is not clean
    # TODO better exception handling
    except Exception as e:
        for io in cell.get_inputs_recursively():
            if isinstance(io, InternalInput):
                io.clear_value()
        raise e


def get_values(cell: "ICell", model: Type[BaseModel], encoding_manager: Optional[EncodingManager] = None) -> BaseModel:
    items: Dict = {}
    for key, model_field in model.__fields__.items():
        if isinstance(cell, ICompositeCell) and isinstance(model_field, BaseModel):
            try:
                internal_cell = cell.get_internal_cell_with_name(key)        # TODO may raise exceptions
            except KeyError:
                pass
            else:
                items[key] = get_values(internal_cell, type(model_field))
                continue
        io = cell.get_output(key)       # TODO may raise exceptions
        value = io.get_value()
        assert isinstance(io, (RuntimeParameter, ConfigParameter, InternalOutput))
        encoded_value = encoding_manager.encode(value, io) if encoding_manager is not None else value
        items[key] = encoded_value
    output_values = model.construct(**items)
    return output_values
