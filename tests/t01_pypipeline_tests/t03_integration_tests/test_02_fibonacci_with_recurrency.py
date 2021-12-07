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

from typing import Generic, TypeVar, Optional, TYPE_CHECKING, List
from pprint import pprint

from pypipeline.cell import Pipeline, ASingleCell
from pypipeline.cellio import Input, Output
from pypipeline.connection import Connection

if TYPE_CHECKING:
    from pypipeline.cell import ICompositeCell


class FibonacciCell(ASingleCell):
    """
    Cell calculating fibonacci using its own outputs from previous timesteps
    (-> recurrent pipeline connections)
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str):
        super(FibonacciCell, self).__init__(parent_cell, name=name)
        self.t_minus_one: Input[int] = Input(self, "t_minus_one")
        self.t_minus_two: Input[int] = Input(self, "t_minus_two")
        # set an initial value, otherwise recurrent connection would throw an UnsetRecurrentConnectionException
        # upon a recurrent call
        self.t: Output[int] = Output(self, "t", initial_value=1)

    def supports_scaling(self) -> bool:
        return False

    def _on_pull(self) -> None:
        tmin2 = self.t_minus_two.pull()
        tmin1 = self.t_minus_one.pull()
        t = tmin2 + tmin1
        self.logger.debug(f"Got {tmin2} and {tmin1} -> returning {t}")
        self.t.set_value(t)


T = TypeVar("T")


class MemoryCell(ASingleCell, Generic[T]):
    """
    Outputs its inputs from the previous run.
    """

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str, initial_value: T):
        super(MemoryCell, self).__init__(parent_cell, name=name)
        self.input: Input[T] = Input(self, "input")
        self.output: Output[T] = Output(self, "output")

        self.prev_input: T = initial_value

    def supports_scaling(self) -> bool:
        return False

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "MemoryCell[T]":
        return MemoryCell(new_parent, name=self.get_name(), initial_value=self.prev_input)

    def _on_pull(self) -> None:
        result: T = self.prev_input
        self.prev_input = self.input.pull()
        self.logger.debug(f"{self}: receiving {self.prev_input}, returning {result}")
        self.output.set_value(result)


class ToplevelPipeline(Pipeline):

    def __init__(self) -> None:
        super(ToplevelPipeline, self).__init__(None, "toplevel")

        self.fib: FibonacciCell = FibonacciCell(self, "fib")
        self.mem: MemoryCell[int] = MemoryCell(self, "mem", initial_value=0)

        self.c1 = Connection(self.fib.t, self.fib.t_minus_one)          # first recurrent connection
        self.c2 = Connection(self.fib.t, self.mem.input)                # second recurrent connection
        self.c3 = Connection(self.mem.output, self.fib.t_minus_two)


def target_fibonacci(length: int) -> List[int]:
    result: List[int] = []
    t_imin2 = 0
    t_imin1 = 1
    for i in range(length):
        t_i = t_imin2 + t_imin1
        result.append(t_i)
        t_imin2 = t_imin1
        t_imin1 = t_i
    return result


def test_fibonacci() -> None:
    toplevel_pipeline = ToplevelPipeline()
    toplevel_pipeline.assert_is_valid()
    toplevel_pipeline.deploy()
    toplevel_pipeline.assert_is_valid()

    print("---- Topology description ----")
    pprint(toplevel_pipeline.get_topology_description(), width=160)
    assert toplevel_pipeline.c1.is_recurrent()
    assert toplevel_pipeline.c2.is_recurrent()
    assert not toplevel_pipeline.c3.is_recurrent()

    assert toplevel_pipeline.mem.is_source_cell()
    assert not toplevel_pipeline.mem.is_sink_cell()
    assert not toplevel_pipeline.fib.is_source_cell()
    assert toplevel_pipeline.fib.is_sink_cell()
    print("------------------------------")

    fibonacci_series = target_fibonacci(22)

    for i, t_i in enumerate(fibonacci_series):
        toplevel_pipeline.pull()
        result = toplevel_pipeline.fib.t.get_value()
        print(f"result: {result}")
        assert result == t_i

    toplevel_pipeline.undeploy()
    toplevel_pipeline.delete()
