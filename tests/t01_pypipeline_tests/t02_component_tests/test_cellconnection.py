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
from typing import Optional

from pypipeline.connection import Connection
from pypipeline.cell import ASingleCell, ICompositeCell, Pipeline
from pypipeline.cellio import (IConnectionExitPoint, IConnectionEntryPoint, Input, Output, InputPort, OutputPort,
                               RuntimeParameter, ConfigParameter, InternalInput, InternalOutput, )
from pypipeline.exceptions import IndeterminableTopologyException


class DummyCell1(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummyCell1, self).__init__(parent_cell, name=name)
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


class DummyCell2(ASingleCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(DummyCell2, self).__init__(parent_cell, name=name)
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


class DummyPipeline(Pipeline):

    def __init__(self):
        super(DummyPipeline, self).__init__(None, name="toplevel")
        self.cell1 = DummyCell1(self, "cell1")
        self.cell2 = DummyCell2(self, "cell2")

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
def pipeline() -> DummyPipeline:
    return DummyPipeline()


@pytest.fixture()
def toplevel_cell_1() -> DummyCell1:
    return DummyCell1(None, "toplevel_cell1")


@pytest.fixture()
def toplevel_cell_2() -> DummyCell1:
    return DummyCell1(None, "toplevel_cell2")


@pytest.fixture()
def pipeline_2() -> DummyPipeline:
    return DummyPipeline()


def test_connection_validation_source_outputs(toplevel_cell_1: DummyCell1) -> None:
    for cell_output in toplevel_cell_1.get_outputs():
        source_ok = Connection.can_have_as_source(cell_output)
        assert source_ok.value == isinstance(cell_output, IConnectionExitPoint)


def test_connection_validation_source_inputs(pipeline: DummyPipeline) -> None:
    for pipeline_input in pipeline.get_inputs():
        source_ok = Connection.can_have_as_source(pipeline_input)
        assert source_ok.value == isinstance(pipeline_input, IConnectionExitPoint)


def test_connection_validation_source_none(pipeline: DummyPipeline) -> None:
    # None as target is not allowed
    assert not Connection.can_have_as_source(None)


def test_connection_validation_target_inputs(toplevel_cell_1: DummyCell1) -> None:
    for cell_input in toplevel_cell_1.get_inputs():
        target_ok = Connection.can_have_as_target(cell_input)
        assert target_ok.value == isinstance(cell_input, IConnectionEntryPoint)


def test_connection_validation_target_outputs(pipeline: DummyPipeline) -> None:
    for pipeline_output in pipeline.get_outputs():
        target_ok = Connection.can_have_as_target(pipeline_output)
        assert target_ok.value == isinstance(pipeline_output, IConnectionEntryPoint)


def test_connection_validation_target_none(pipeline: DummyPipeline) -> None:
    # None as target is not allowed
    assert not Connection.can_have_as_target(None)


def test_connection_intercell_inside_parent_non_recurrent(pipeline: DummyPipeline) -> None:
    for cell_output in pipeline.cell1.get_outputs():
        for cell_input in pipeline.cell2.get_inputs():
            ok = Connection.can_have_as_source_and_target(cell_output, cell_input)
            assert ok.value == (isinstance(cell_output, IConnectionExitPoint) and
                                isinstance(cell_input, IConnectionEntryPoint))
            if ok:
                conn = Connection(cell_output, cell_input)
                conn.assert_is_valid()
                assert conn.is_inter_cell_connection()
                assert not conn.is_intra_cell_connection()
                if cell_output.has_initial_value():
                    # Connection with initial value that can be both recurrent or non-recurrent
                    with pytest.raises(IndeterminableTopologyException):
                        conn.is_recurrent()
                    conn.delete()
                    conn = Connection(cell_output, cell_input, explicitly_mark_as_recurrent=True)
                    conn.assert_is_valid()
                    assert conn.is_inter_cell_connection()
                    assert not conn.is_intra_cell_connection()
                    assert conn.is_recurrent()
                    conn.delete()
                    conn = Connection(cell_output, cell_input, explicitly_mark_as_recurrent=False)
                    conn.assert_is_valid()
                    assert conn.is_inter_cell_connection()
                    assert not conn.is_intra_cell_connection()
                    assert not conn.is_recurrent()
                else:
                    assert not conn.is_recurrent()
                conn.delete()


def test_connection_intercell_inside_parent_recurrent(pipeline: DummyPipeline) -> None:
    for cell_output in pipeline.cell1.get_outputs():
        for cell_input in pipeline.cell1.get_inputs():
            ok = Connection.can_have_as_source_and_target(cell_output, cell_input)
            assert ok.value == (isinstance(cell_output, IConnectionExitPoint) and
                                isinstance(cell_input, IConnectionEntryPoint))
            if ok:
                conn = Connection(cell_output, cell_input)
                conn.assert_is_valid()
                assert conn.is_inter_cell_connection()
                assert not conn.is_intra_cell_connection()
                if cell_output.has_initial_value():
                    assert conn.is_recurrent()
                else:
                    # recurrent connection without initial value
                    with pytest.raises(IndeterminableTopologyException):
                        conn.is_recurrent()
                conn.delete()


def test_connection_intercell_outside_parent_non_recurrent(toplevel_cell_1: DummyCell1,
                                                           toplevel_cell_2: DummyCell2) -> None:
    # Connections outside a parent cell are not allowed.
    for cell_output in toplevel_cell_1.get_outputs():
        for cell_input in toplevel_cell_2.get_inputs():
            assert not Connection.can_have_as_source_and_target(cell_output, cell_input)


def test_connection_intercell_outside_parent_recurrent(toplevel_cell_1: DummyCell1) -> None:
    # Connections outside a parent cell are not allowed.
    for cell_output in toplevel_cell_1.get_outputs():
        for cell_input in toplevel_cell_1.get_inputs():
            assert not Connection.can_have_as_source_and_target(cell_output, cell_input)


def test_connection_intracell_inputport_to_outputport(pipeline: DummyPipeline) -> None:
    for pipeline_input in pipeline.get_inputs():
        for pipeline_output in pipeline.get_outputs():
            ok = Connection.can_have_as_source_and_target(pipeline_input, pipeline_output)
            assert ok.value == (isinstance(pipeline_input, IConnectionExitPoint) and
                                isinstance(pipeline_output, IConnectionEntryPoint))
            if ok:
                conn = Connection(pipeline_input, pipeline_output)
                conn.assert_is_valid()
                assert not conn.is_inter_cell_connection()
                assert conn.is_intra_cell_connection()
                assert not conn.is_recurrent()
                conn.delete()


def test_connection_intracell_inputport_normal(pipeline: DummyPipeline) -> None:
    for pipeline_input in pipeline.get_inputs():
        for cell_input in pipeline.cell1.get_inputs():
            ok = Connection.can_have_as_source_and_target(pipeline_input, cell_input)
            assert ok.value == (isinstance(pipeline_input, IConnectionExitPoint) and
                                isinstance(cell_input, IConnectionEntryPoint))
            if ok:
                conn = Connection(pipeline_input, cell_input)
                conn.assert_is_valid()
                assert not conn.is_inter_cell_connection()
                assert conn.is_intra_cell_connection()
                assert not conn.is_recurrent()
                conn.delete()


def test_connection_intracell_inputport_both_source_and_target(pipeline: DummyPipeline) -> None:
    # Not allowed
    for pipeline_input in pipeline.get_inputs():
        assert not Connection.can_have_as_source_and_target(pipeline_input, pipeline_input)


def test_connection_intracell_inputport_as_target_from_internal_cell(pipeline: DummyPipeline) -> None:
    # Not allowed
    for cell_output in pipeline.cell1.get_outputs():
        for pipeline_input in pipeline.get_inputs():
            assert not Connection.can_have_as_source_and_target(cell_output, pipeline_input)


def test_connection_intracell_outputport_normal(pipeline: DummyPipeline) -> None:
    for cell_output in pipeline.cell1.get_outputs():
        for pipeline_output in pipeline.get_outputs():
            ok = Connection.can_have_as_source_and_target(cell_output, pipeline_output)
            assert ok.value == (isinstance(cell_output, IConnectionExitPoint) and
                                isinstance(pipeline_output, IConnectionEntryPoint))
            if ok:
                conn = Connection(cell_output, pipeline_output)
                conn.assert_is_valid()
                assert not conn.is_inter_cell_connection()
                assert conn.is_intra_cell_connection()
                assert not conn.is_recurrent()
                conn.delete()


def test_connection_intracell_outputport_both_source_and_target(pipeline: DummyPipeline) -> None:
    # Not allowed
    for pipeline_output in pipeline.get_outputs():
        assert not Connection.can_have_as_source_and_target(pipeline_output, pipeline_output)


def test_connection_intracell_outputport_as_source_to_internal_cell(pipeline: DummyPipeline) -> None:
    # Not allowed
    for pipeline_output in pipeline.get_outputs():
        for cell_input in pipeline.cell1.get_inputs():
            assert not Connection.can_have_as_source_and_target(pipeline_output, cell_input)


def test_connection_illegal_parent(pipeline: DummyPipeline, pipeline_2: DummyPipeline) -> None:
    assert not Connection.can_have_as_source_and_target(pipeline.cell1.output,
                                                        pipeline_2.cell2.input)


def test_connection_intercell_inside_parent_invalid_recurrency_marking(pipeline: DummyPipeline) -> None:
    conn = Connection(pipeline.cell1.output, pipeline.cell2.input, explicitly_mark_as_recurrent=False)
    conn.assert_is_valid()
    assert not conn.is_recurrent()
    conn.delete()

    conn = Connection(pipeline.cell1.output, pipeline.cell2.input, explicitly_mark_as_recurrent=True)
    conn.assert_is_valid()
    with pytest.raises(IndeterminableTopologyException):
        conn.is_recurrent()
    conn.delete()


# TODO test connections that are recurrent or not recurrent based on other connections.
# TODO test connections that are recurrent over multiple cells

# TODO recurrent connections inside a scalablecell (=with a scalablecell as direct parent) should not be possible!
# TODO recurrent connections should force a cell/pipeline to be not-scalable.

# TODO test pulling the connections (ex: with None values)
