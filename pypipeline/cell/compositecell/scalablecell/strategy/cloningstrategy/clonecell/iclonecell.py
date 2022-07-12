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

from typing import Sequence, TYPE_CHECKING, Optional

from pypipeline.cell.compositecell.icompositecell import ICompositeCell

if TYPE_CHECKING:
    from prometheus_client import Histogram

    from pypipeline.validation import BoolExplained
    from pypipeline.cell.icell import ICell
    from pypipeline.cellio import InternalInput, InternalOutput


class ICloneCell(ICompositeCell):
    """
    Clone cell interface.

    Controlling class in the IObserver-IObservable relation, as observer of the original cell.
    -> Relation should be created/deleted in the __init__ and delete() method.
    """

    @classmethod
    def create(cls, original_cell: "ICell", name: str) -> "ICloneCell":
        """
        Factory method to create a new clone.

        Args:
            original_cell: the original cell to be cloned.
            name: the name of the new clone cell.
        Returns:
            A new clone cell.
        Raises:
            InvalidInputException
            NotImplementedError: if the original cell doesn't support cloning.
        """
        raise NotImplementedError

    # ------ Original cell ------

    def get_original_cell(self) -> "ICell":
        """
        Returns:
            The original cell, of which this cell is a clone.
        """
        raise NotImplementedError

    def can_have_as_original_cell(self, original_cell: "ICell") -> "BoolExplained":
        """
        Args:
            original_cell: original cell to validate.
        Returns:
            TrueExplained if the given cell is a valid original cell for this clone cell. FalseExplained otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_original_cell(self) -> None:
        """
        Raises:
            InvalidStateException: if the original cell is invalid.
        """
        raise NotImplementedError

    # ------ Inputs & Outputs ------

    def get_clone_inputs(self) -> "Sequence[InternalInput]":
        """
        Returns:
            The (internal) clone inputs of this clone cell.
        """
        raise NotImplementedError

    def get_clone_input(self, name: str) -> "InternalInput":
        """
        Args:
            name: the name of the clone input to get.
        Returns:
            The (internal) clone input with the given name.
        Raises:
             KeyError: if this cell has no clone input with the given name.
        """
        raise NotImplementedError

    def get_clone_outputs(self) -> "Sequence[InternalOutput]":
        """
        Returns:
            The (internal) clone outputs of this clone cell.
        """
        raise NotImplementedError

    def get_clone_output(self, name: str) -> "InternalOutput":
        """
        Args:
            name: the name of the clone output to get.
        Returns:
            The (internal) clone output with the given name.
        Raises:
             KeyError: if this cell has no clone output with the given name.
        """
        raise NotImplementedError

    def _get_pull_duration_metric(self) -> Optional["Histogram"]:
        raise NotImplementedError

    def _set_pull_duration_metric(self, metric: Optional["Histogram"]) -> None:
        raise NotImplementedError
