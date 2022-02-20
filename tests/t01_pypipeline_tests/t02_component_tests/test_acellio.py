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

import pytest
from typing import Optional, List, Type

from pypipeline.connection import Connection
from pypipeline.cell import ASingleCell, ICompositeCell, Pipeline, ScalableCell, ICell
from pypipeline.cellio import (IConnectionExitPoint, IConnectionEntryPoint, Input, Output, InputPort, OutputPort,
                               RuntimeParameter, ConfigParameter, InternalInput, InternalOutput, IO)
from pypipeline.exceptions import IndeterminableTopologyException, InvalidInputException


single_io_types: List[Type[IO]] = [Input, Output, RuntimeParameter, ConfigParameter]
composite_io_types = [InputPort, InternalInput, OutputPort, InternalOutput]


class DummyCell1(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummyCell1, self).__init__(parent_cell, name=name)

    def _on_pull(self) -> None:
        pass

    def supports_scaling(self) -> bool:
        raise NotImplementedError


class DummyPipeline(Pipeline):

    def __init__(self):
        super(DummyPipeline, self).__init__(None, name="pipeline")


class DummyScalableCell(ScalableCell):

    def __init__(self):
        super(DummyScalableCell, self).__init__(None, name="scalablecell")


@pytest.fixture()
def dummycell1() -> DummyCell1:
    return DummyCell1(None, "dummycell1")


@pytest.fixture()
def pipeline() -> DummyPipeline:
    return DummyPipeline()


@pytest.fixture()
def scalable_cell() -> DummyScalableCell:
    return DummyScalableCell()


@pytest.fixture()
def composite_cells(pipeline: DummyPipeline,
                    scalable_cell) -> List[ICompositeCell]:
    return [pipeline, scalable_cell]


@pytest.fixture()
def single_cells(dummycell1: DummyCell1) -> List[ASingleCell]:
    return [dummycell1]


@pytest.fixture()
def all_cells(composite_cells: List[ICompositeCell],
              single_cells: List[ASingleCell]) -> List[ICell]:
    result: List[ICell] = [*composite_cells, *single_cells]
    return result


def test_acellio_validation_name() -> None:
    for io_type in single_io_types:
        assert not io_type.can_have_as_name(None)
        assert not io_type.can_have_as_name("")
        assert not io_type.can_have_as_name("abc.")
        assert io_type.can_have_as_name("abc_de-fg(456)")


# def test_acellio_validation_cell(all_cells: List[ICell]) -> None:
#     for io_type in single_io_types:
#         assert not io_type.can_have_as_cell(None)
#         for cell in all_cells:
#             assert io_type.can_have_as_cell(cell)


def test_cellio_creation() -> None:
    for io_type in single_io_types:
        cell = DummyCell1(None, "dummy")
        io: IO = io_type(cell, "a_name")
        assert io.get_name() == "a_name"
        assert io.get_full_name() == cell.get_full_name() + ".a_name"
        assert io.get_cell() == cell
        io.assert_is_valid()


def test_cell_creation_invalid_parent() -> None:
    for io_type in single_io_types:
        with pytest.raises(InvalidInputException):
            io_type(None, "a_name")


def test_cell_creation_invalid_name() -> None:
    for io_type in single_io_types:
        cell = DummyCell1(None, "dummy")
        with pytest.raises(InvalidInputException):
            io_type(cell, "")


class DummyCell2(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummyCell2, self).__init__(parent_cell, name=name)
        self.input: Input[int] = Input(self, "input")
        self.output: Output[int] = Output(self, "output")
        self.output_with_initial_value: Output[int] = Output(self, "output_with_initial_value", initial_value=321)

        # Other input types:
        RuntimeParameter(self, "runtime_param")
        ConfigParameter(self, "config_param")

    def _on_pull(self) -> None:
        self.output.set_value(123)

    def supports_scaling(self) -> bool:
        raise NotImplementedError

    def get_nb_available_pulls(self) -> Optional[int]:
        return 456


class DummyCell3(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummyCell3, self).__init__(parent_cell, name=name)
        self.input: Input[int] = Input(self, "input")
        self.output: Output[int] = Output(self, "output")

        # Other input types:
        RuntimeParameter(self, "runtime_param")
        ConfigParameter(self, "config_param")

    def _on_pull(self) -> None:
        self.output.set_value(789)

    def supports_scaling(self) -> bool:
        raise NotImplementedError

    def get_nb_available_pulls(self) -> Optional[int]:
        return 654


class DummyPipeline2(Pipeline):

    def __init__(self):
        super(DummyPipeline2, self).__init__(None, name="toplevel")
        self.cell2 = DummyCell2(self, "cell1")
        self.cell3 = DummyCell3(self, "cell2")

        self.input: InputPort[int] = InputPort(self, "input_port")
        self.output: OutputPort[int] = OutputPort(self, "output_port")

        # Other input types:
        Input(self, "input")
        RuntimeParameter(self, "runtime_param")
        ConfigParameter(self, "config_param")
        InternalInput(self, "internal_input")

        # other output types:
        Output(self, "output")
        InternalOutput(self, "internal_output")


@pytest.fixture()
def dummy_pipeline_2() -> DummyPipeline2:
    return DummyPipeline2()


def test_acellio_add_connection(dummy_pipeline_2: DummyPipeline2) -> None:
    assert len(dummy_pipeline_2.cell3.input.get_incoming_connections()) == 0
    assert len(dummy_pipeline_2.cell2.output.get_outgoing_connections()) == 0
    dummy_pipeline_2.cell3.input.assert_has_proper_incoming_connections()
    dummy_pipeline_2.cell2.output.assert_has_proper_outgoing_connections()
    dummy_pipeline_2.cell2.assert_is_valid()
    dummy_pipeline_2.cell3.assert_is_valid()
    c = Connection(dummy_pipeline_2.cell2.output, dummy_pipeline_2.cell3.input)
    assert c in dummy_pipeline_2.cell3.input.get_incoming_connections()
    assert c in dummy_pipeline_2.cell2.output.get_outgoing_connections()
    assert dummy_pipeline_2.cell3.input.has_as_incoming_connection(c)
    assert dummy_pipeline_2.cell2.output.has_as_outgoing_connection(c)
    dummy_pipeline_2.cell3.input.assert_has_proper_incoming_connections()
    dummy_pipeline_2.cell2.output.assert_has_proper_outgoing_connections()
    assert c == dummy_pipeline_2.cell2.output.get_outgoing_connection_to(dummy_pipeline_2.cell3.input)
    assert c == dummy_pipeline_2.cell3.input.get_incoming_connection_with(dummy_pipeline_2.cell2.output)
    dummy_pipeline_2.cell2.assert_is_valid()
    dummy_pipeline_2.cell3.assert_is_valid()
    c.delete()
    assert len(dummy_pipeline_2.cell3.input.get_incoming_connections()) == 0
    assert len(dummy_pipeline_2.cell2.output.get_outgoing_connections()) == 0
    dummy_pipeline_2.cell3.input.assert_has_proper_incoming_connections()
    dummy_pipeline_2.cell2.output.assert_has_proper_outgoing_connections()
    dummy_pipeline_2.cell2.assert_is_valid()
    dummy_pipeline_2.cell3.assert_is_valid()


# # TODO test pulling
