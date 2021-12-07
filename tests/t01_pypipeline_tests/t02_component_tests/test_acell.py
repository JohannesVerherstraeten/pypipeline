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

import pytest
from typing import Optional, List

from pypipeline.connection import Connection
from pypipeline.cell import ASingleCell, ICompositeCell, Pipeline, ScalableCell, ICell
from pypipeline.cellio import (IConnectionExitPoint, IConnectionEntryPoint, Input, Output, InputPort, OutputPort,
                               RuntimeParameter, ConfigParameter, InternalInput, InternalOutput, )
from pypipeline.exceptions import IndeterminableTopologyException, InvalidInputException, AlreadyDeployedException


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
                    scalable_cell: DummyScalableCell) -> List[ICompositeCell]:
    return [pipeline, scalable_cell]


@pytest.fixture()
def single_cells(dummycell1: DummyCell1) -> List[ASingleCell]:
    return [dummycell1]


@pytest.fixture()
def all_cells(composite_cells: List[ICompositeCell],
              single_cells: List[ASingleCell]) -> List[ICell]:
    result: List[ICell] = [*composite_cells, *single_cells]
    return result


def test_cell_validation_name(all_cells: List[ICell]) -> None:
    for cell in all_cells:
        assert not cell.can_have_as_name(None)
        assert not cell.can_have_as_name("")
        assert not cell.can_have_as_name("abc.")
        assert cell.can_have_as_name("abc_de-fg(456)")


def test_cell_validation_parent(all_cells: List[ICell],
                                composite_cells: List[ICompositeCell],
                                single_cells: List[ASingleCell]) -> None:
    for cell in all_cells:
        assert cell.can_have_as_parent_cell(None)
        for composite_cell in composite_cells:
            assert cell.can_have_as_parent_cell(composite_cell)
        for single_cell in single_cells:
            assert not cell.can_have_as_parent_cell(single_cell)


def test_cell_creation_no_parent() -> None:
    cell_types = [DummyCell1, Pipeline, ScalableCell]
    for cell_type in cell_types:
        dummy = cell_type(None, "a_name")
        assert dummy.get_name() == "a_name"
        assert dummy.get_full_name() == "a_name"
        assert dummy.get_parent_cell() is None
        dummy.assert_is_valid()


def test_cell_creation_parent() -> None:
    composite_cell_types = [Pipeline, ScalableCell]
    cell_types = [DummyCell1, Pipeline, ScalableCell]
    for composite_cell_type in composite_cell_types:
        for cell_type in cell_types:
            parent_cell = composite_cell_type(None, "parent_name")
            dummy = cell_type(parent_cell, "a_name")
            assert dummy.get_name() == "a_name"
            assert dummy.get_full_name() == parent_cell.get_name() + ".a_name"
            assert dummy.get_parent_cell() == parent_cell
            dummy.assert_is_valid()
            parent_cell.assert_is_valid()


def test_cell_creation_invalid_parent() -> None:
    single_cell = DummyCell1(None, "dummy_single_cell")
    cell_types = [DummyCell1, Pipeline, ScalableCell]
    for cell_type in cell_types:
        with pytest.raises(InvalidInputException):
            cell_type(single_cell, "a_name")


def test_cell_creation_invalid_name() -> None:
    cell_types = [DummyCell1, Pipeline, ScalableCell]
    for cell_type in cell_types:
        parent_cell = Pipeline(None, "parent_name")
        with pytest.raises(InvalidInputException):
            cell_type(parent_cell, "")


def test_cell_add_input_ok(all_cells: List[ICell]) -> None:
    for cell in all_cells:
        test_input: Input[int] = Input(cell, "input_name")
        assert test_input in cell.get_inputs()
        assert "input_name" in cell.get_input_names()
        assert test_input == cell.get_input("input_name")
        assert test_input == cell.get_io("input_name")
        assert cell.has_as_input(test_input)
        cell.assert_has_proper_io()


def test_cell_add_input_when_deployed(dummycell1: DummyCell1) -> None:
    dummycell1.deploy()
    with pytest.raises(AlreadyDeployedException):
        Input(dummycell1, "input_name")


def test_cell_add_input_input_name_already_exists(all_cells: List[ICell]) -> None:
    for cell in all_cells:
        Input(cell, "input_name")
        with pytest.raises(InvalidInputException):
            Input(cell, "input_name")


def test_cell_add_input_output_name_already_exists(all_cells: List[ICell]) -> None:
    for cell in all_cells:
        Output(cell, "output_name")
        with pytest.raises(InvalidInputException):
            Input(cell, "output_name")


def test_cell_add_output_ok(all_cells: List[ICell]) -> None:
    for cell in all_cells:
        test_output: Output[int] = Output(cell, "output_name")
        assert test_output in cell.get_outputs()
        assert "output_name" in cell.get_output_names()
        assert test_output == cell.get_output("output_name")
        assert test_output == cell.get_io("output_name")
        assert cell.has_as_output(test_output)
        cell.assert_has_proper_io()


def test_cell_add_output_when_deployed(dummycell1: DummyCell1) -> None:
    dummycell1.deploy()
    with pytest.raises(AlreadyDeployedException):
        Output(dummycell1, "output_name")


def test_cell_add_output_output_name_already_exists(all_cells: List[ICell]) -> None:
    for cell in all_cells:
        Output(cell, "output_name")
        with pytest.raises(InvalidInputException):
            Output(cell, "output_name")


def test_cell_add_output_input_name_already_exists(all_cells: List[ICell]) -> None:
    for cell in all_cells:
        Input(cell, "input_name")
        with pytest.raises(InvalidInputException):
            Output(cell, "input_name")


class DummySource(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummySource, self).__init__(parent_cell, name)
        self.output: Output[int] = Output(self, "output")

    def _on_pull(self) -> None:
        self.output.set_value(39)


class DummyProcessor(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummyProcessor, self).__init__(parent_cell, name)
        self.input: Input[int] = Input(self, "input")
        self.output: Output[int] = Output(self, "output")

    def _on_pull(self) -> None:
        value = self.input.pull()
        self.output.set_value(value * 2)


class DummySink(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummySink, self).__init__(parent_cell, name)
        self.input: Input[int] = Input(self, "input")

    def _on_pull(self) -> None:
        self.input.pull()


class DummyPipeline2(Pipeline):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummyPipeline2, self).__init__(parent_cell, name)
        self.source = DummySource(self, "source")
        self.processor = DummyProcessor(self, "processor")
        self.sink = DummySink(self, "sink")
        Connection(self.source.output, self.processor.input)
        Connection(self.processor.output, self.sink.input)


@pytest.fixture()
def dummy_pipeline_2() -> DummyPipeline2:
    return DummyPipeline2(None, "pipeline")


def test_cell_topology_set_clear(dummy_pipeline_2: DummyPipeline2) -> None:
    assert not dummy_pipeline_2._internal_topology_is_set()
    dummy_pipeline_2._set_internal_topology()
    assert dummy_pipeline_2._internal_topology_is_set()
    dummy_pipeline_2._clear_internal_topology()
    assert not dummy_pipeline_2._internal_topology_is_set()


def test_cell_topology_toplevel_cell(dummy_pipeline_2: DummyPipeline2) -> None:
    assert dummy_pipeline_2.is_source_cell()
    assert dummy_pipeline_2.is_sink_cell()


def test_cell_topology_source_sink_processor(dummy_pipeline_2: DummyPipeline2) -> None:
    assert dummy_pipeline_2.source.is_source_cell()
    assert not dummy_pipeline_2.source.is_sink_cell()
    assert not dummy_pipeline_2.processor.is_source_cell()
    assert not dummy_pipeline_2.processor.is_sink_cell()
    assert not dummy_pipeline_2.sink.is_source_cell()
    assert dummy_pipeline_2.sink.is_sink_cell()


def test_cell_topology_description(dummy_pipeline_2: DummyPipeline2) -> None:
    for cell in list(dummy_pipeline_2.get_internal_cells()) + [dummy_pipeline_2]:
        description = cell.get_topology_description()
        assert description["cell"] == str(cell)
        assert description["is_source_cell"] == cell.is_source_cell()
        assert description["is_sink_cell"] == cell.is_sink_cell()


def test_cell_observers(dummy_pipeline_2: DummyPipeline2) -> None:
    assert len(dummy_pipeline_2.get_observers()) == 0
    assert dummy_pipeline_2 in dummy_pipeline_2.source.get_observers()
    assert dummy_pipeline_2 in dummy_pipeline_2.processor.get_observers()
    assert dummy_pipeline_2 in dummy_pipeline_2.sink.get_observers()
    assert dummy_pipeline_2.source.has_as_observer(dummy_pipeline_2)
    assert dummy_pipeline_2.sink.has_as_observer(dummy_pipeline_2)


def test_cell_observers_validation(dummy_pipeline_2: DummyPipeline2) -> None:
    for cell in list(dummy_pipeline_2.get_internal_cells()) + [dummy_pipeline_2]:
        cell.assert_has_proper_observers()


def test_cell_sync_state(dummy_pipeline_2: DummyPipeline2,
                         all_cells: List[ICell]) -> None:
    for cell in all_cells + [dummy_pipeline_2]:
        state = cell._get_sync_state()
        cell._set_sync_state(state)


def test_cell_clone(dummy_pipeline_2: DummyPipeline2) -> None:
    for cell in list(dummy_pipeline_2.get_internal_cells()) + [dummy_pipeline_2]:
        cell_clone = cell.clone(None)


def test_cell_deploy(dummy_pipeline_2: DummyPipeline2) -> None:
    dummy_pipeline_2.assert_is_valid()
    assert not dummy_pipeline_2.is_deployed()
    assert not dummy_pipeline_2.source.is_deployed()
    dummy_pipeline_2.deploy()
    dummy_pipeline_2.assert_is_valid()
    assert dummy_pipeline_2.is_deployed()
    assert dummy_pipeline_2.source.is_deployed()
    dummy_pipeline_2.undeploy()
    dummy_pipeline_2.assert_is_valid()
    assert not dummy_pipeline_2.is_deployed()
    assert not dummy_pipeline_2.source.is_deployed()


# TODO test pulling
