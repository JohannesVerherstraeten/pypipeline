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

from typing import Sequence, TYPE_CHECKING
from abc import ABC, abstractmethod

from pypipeline.cell.compositecell.icompositecell import ICompositeCell

if TYPE_CHECKING:
    from pypipeline.validation import BoolExplained
    from pypipeline.cell.icell import ICell
    from pypipeline.cellio import InternalInput, InternalOutput


class ICloneCell(ICompositeCell, ABC):
    """
    Clone cell interface.

    Controlling class in the IObserver-IObservable relation, as observer of the original cell.
    -> Relation should be created/deleted in the __init__ and delete() method.
    """

    @classmethod
    @abstractmethod
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

    @abstractmethod
    def get_original_cell(self) -> "ICell":
        """
        Returns:
            The original cell, of which this cell is a clone.
        """
        raise NotImplementedError

    @abstractmethod
    def can_have_as_original_cell(self, original_cell: "ICell") -> "BoolExplained":
        """
        Args:
            original_cell: original cell to validate.
        Returns:
            TrueExplained if the given cell is a valid original cell for this clone cell. FalseExplained otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def assert_has_proper_original_cell(self) -> None:
        """
        Raises:
            InvalidStateException: if the original cell is invalid.
        """
        raise NotImplementedError

    # ------ Inputs & Outputs ------

    @abstractmethod
    def get_clone_inputs(self) -> "Sequence[InternalInput]":
        """
        Returns:
            The (internal) clone inputs of this clone cell.
        """
        raise NotImplementedError

    @abstractmethod
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

    @abstractmethod
    def get_clone_outputs(self) -> "Sequence[InternalOutput]":
        """
        Returns:
            The (internal) clone outputs of this clone cell.
        """
        raise NotImplementedError

    @abstractmethod
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
