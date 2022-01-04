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

from typing import TypeVar, Generic, Dict, Any, Optional, TYPE_CHECKING
from logging import getLogger

import pypipeline
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not
from pypipeline.exceptions import InvalidInputException, IndeterminableTopologyException, InvalidStateException, \
    CannotBeDeletedException
from pypipeline.cellio import IConnectionEntryPoint, IConnectionExitPoint, IInput, IOutput
from pypipeline.connection.iconnection import IConnection

if TYPE_CHECKING:
    from pypipeline.cell import ICompositeCell


T = TypeVar('T')


class Connection(IConnection[T], Generic[T]):
    """
    Connection class.

    A connection connects an IConnectionExitPoint with an IConnectionEntryPoint. Usually: an Output with an Input.

    An IConnection is owned by its parent cell.

    An IConnection is the controlling class in the IConnection-ICompositeCell relation, as internal connection of the
    composite cell.
    -> Main mutators can be found in __init__ and delete()

    An IConnection is the controlling class in the IConnection-IConnectionEntryPoint relation, as an incoming
    connection of the entry point.
    -> Main mutators can be found in __init__ and delete()

    An IConnection is the controlling class in the IConnection-IConnectionExitPoint relation, as an outgoing
    connection of the exit point.
    -> Main mutators can be found in __init__ and delete()
    """

    def __init__(self,
                 source: "IConnectionExitPoint[T]",
                 target: "IConnectionEntryPoint[T]",
                 explicitly_mark_as_recurrent: Optional[bool] = None):
        """
        Args:
            source: the source (or start) of the new connection.
            target: the target (or end) of the new connection.
            explicitly_mark_as_recurrent: explicitly mark this connection as (non-)recurrent. If None, PyPipeline will
                try to infer the recurrency automatically.
        """
        self.logger = getLogger(self.__class__.__name__)

        # Main mutator in the IConnection-IConnectionEntryPoint relation, as an incoming connection of the entry point.
        # Main mutator in the IConnection-IConnectionExitPoint relation, as an outgoing connection of the exit point.
        # Main mutator in the IConnection-ICompositeCell relation, as internal connection of the composite cell.
        parent_cell = _get_parent_cell(source, target)
        raise_if_not(self.can_have_as_source_and_target(source, target), InvalidInputException,
                     f"Cannot create a connection between {source} and {target}: ")
        raise_if_not(self.can_have_as_parent_cell(parent_cell), InvalidInputException)

        target._add_incoming_connection(self)           # Access to protected member on purpose  # May raise exceptions
        try:
            source._add_outgoing_connection(self)       # Access to protected member on purpose  # May raise exceptions
        except Exception as e:
            target._remove_incoming_connection(self)    # Rollback
            raise e
        try:
            parent_cell._add_internal_connection(self)  # Access to protected member on purpose  # May raise exceptions
        except Exception as e:
            source._remove_outgoing_connection(self)    # Rollback
            target._remove_incoming_connection(self)    # Rollback
            raise e
        self.__target = target
        self.__source = source
        self.__parent_cell: "ICompositeCell" = parent_cell

        self.__explicitly_mark_as_recurrent = explicitly_mark_as_recurrent
        self.__is_deployed: bool = False

    def get_source(self) -> "IConnectionExitPoint[T]":
        return self.__source

    @classmethod
    def can_have_as_source(cls, source: "IConnectionExitPoint[T]") -> BoolExplained:
        if not isinstance(source, pypipeline.cellio.IConnectionExitPoint):
            return FalseExplained(f"{cls} expects an instance of sdk.cellio.IConnectionExitPoint as source, "
                                  f"got {source}")
        return TrueExplained()

    def get_target(self) -> "IConnectionEntryPoint[T]":
        return self.__target

    @classmethod
    def can_have_as_target(cls, target: "IConnectionEntryPoint[T]") -> BoolExplained:
        if not isinstance(target, pypipeline.cellio.IConnectionEntryPoint):
            return FalseExplained(f"{cls} expects an instance of sdk.cellio.IConnectionEntryPoint as target, "
                                  f"got {target}")
        return TrueExplained()

    @classmethod
    def can_have_as_source_and_target(cls,
                                      source: "IConnectionExitPoint[T]",
                                      target: "IConnectionEntryPoint[T]") -> BoolExplained:
        src_and_target_ok: BoolExplained = TrueExplained()
        src_and_target_ok *= cls.can_have_as_source(source)
        src_and_target_ok *= cls.can_have_as_target(target)
        if not src_and_target_ok:
            return src_and_target_ok

        try:
            parent_cell = _get_parent_cell(source, target)
        except InvalidInputException as e:
            return FalseExplained(str(e))
        parent_ok = cls.can_have_as_parent_cell(parent_cell)
        if not parent_ok:
            return parent_ok

        # Connections can only exist between 2 cells with the same parent cell (inter-cell connection),
        # or between a composite cell and one of its child cells (intra-cell connection).
        if _is_inter_cell_connection(source, target) == _is_intra_cell_connection(source, target):
            return FalseExplained(f"{cls}: cannot determine the type of connection (inter- or intra cell) that should "
                                  f"be made between the given source {source} and target {target}. ")
        return TrueExplained()

    def assert_has_proper_source_and_target(self) -> None:
        raise_if_not(self.can_have_as_source_and_target(self.get_source(), self.get_target()), InvalidStateException)
        if not self.get_source().has_as_outgoing_connection(self):
            raise InvalidStateException(f"Inconsistent relation: {self.get_source()} doesn't have {self} as outgoing "
                                        f"connection.")
        if not self.get_target().has_as_incoming_connection(self):
            raise InvalidStateException(f"Inconsistent relation: {self.get_target()} doesn't have {self} as incoming "
                                        f"connection.")

    def get_parent_cell(self) -> "ICompositeCell":
        return self.__parent_cell

    @classmethod
    def can_have_as_parent_cell(cls, cell: "ICompositeCell") -> BoolExplained:
        if not isinstance(cell, pypipeline.cell.ICompositeCell):
            return FalseExplained(f"{cls} expects an instance of sdk.cell.ICompositeCell as parent, got {cell}. ")
        return TrueExplained()

    def assert_has_proper_parent_cell(self) -> None:
        raise_if_not(self.can_have_as_parent_cell(self.get_parent_cell()), InvalidStateException)
        if not self.get_parent_cell().has_as_internal_connection(self):
            raise InvalidStateException(f"Inconsistent relation: {self.get_parent_cell()} doesn't have {self} as "
                                        f"internal connection.")

    def is_inter_cell_connection(self) -> bool:
        return _is_inter_cell_connection(self.get_source(), self.get_target())

    def is_intra_cell_connection(self) -> bool:
        return _is_intra_cell_connection(self.get_source(), self.get_target())

    def is_explicitly_marked_as_recurrent(self) -> bool:
        return self.__explicitly_mark_as_recurrent is not None and self.__explicitly_mark_as_recurrent

    def is_explicitly_marked_as_non_recurrent(self) -> bool:
        return self.__explicitly_mark_as_recurrent is not None and not self.__explicitly_mark_as_recurrent

    def is_recurrent(self) -> bool:
        parent_cell = self.get_parent_cell()
        return parent_cell._has_as_internal_recurrent_connection(self)  # access to protected method on purpose  # TODO may raise exceptions

    def get_topology_description(self) -> Dict[str, Any]:
        result = {
            "connection": str(self),
            "connection_type": self.__class__.__name__,
            "source_id": self.get_source().get_full_name(),
            "target_id": self.get_target().get_full_name(),
            "is_recurrent": self.is_recurrent(),
            "is_inter_cell_connection": self.is_inter_cell_connection()
        }
        return result

    def assert_has_proper_topology(self) -> None:
        # The topology should not necessarily be determined already for a connection to be valid.
        # But if it is determined, it should be consistent with the explicit recurrency markings.
        if not self._is_deployed():
            # If not deployed, the pipeline may still be in an unfinished state. -> not necessarily invalid.
            return
        try:
            is_recurrent = self.is_recurrent()
        except IndeterminableTopologyException as e:
            raise InvalidStateException(f"{self} is deployed but doesn't have a topology: {str(e)}")
        else:
            if not is_recurrent and self.is_explicitly_marked_as_recurrent():
                raise InvalidStateException(f"{self} is explicitly marked as recurrent, but is_recurrent() returns "
                                            f"False.")
            if is_recurrent and self.is_explicitly_marked_as_non_recurrent():
                raise InvalidStateException(f"{self} is explicitly marked as non-recurrent, but is_recurrent() returns "
                                            f"True.")

    def _deploy(self) -> None:
        assert not self._is_deployed()
        self.__is_deployed = True

    def _undeploy(self) -> None:
        assert self._is_deployed()
        self.__is_deployed = False

    def _is_deployed(self) -> bool:
        return self.__is_deployed

    def _assert_is_properly_deployed(self) -> None:
        source_is_deployed = self.get_source()._is_deployed()   # Access to protected method on purpose
        target_is_deployed = self.get_target()._is_deployed()   # Access to protected method on purpose
        if self._is_deployed() != (source_is_deployed and target_is_deployed):
            raise InvalidStateException(f"{self} is {'' if self._is_deployed() else 'not '}deployed, but "
                                        f"{self.get_source()} is {'' if source_is_deployed else 'not '}deployed and "
                                        f"{self.get_target()} is {'' if target_is_deployed else 'not '}deployed.")

    def pull(self) -> T:
        # self.logger.warning(f"Pulling {self}")
        return self.get_source().pull_as_connection(self)

    def reset(self) -> None:
        self.get_source().reset()

    def get_nb_available_pulls(self) -> Optional[int]:
        return self.get_source().get_nb_available_pulls()

    def assert_is_valid(self) -> None:
        self.assert_has_proper_source_and_target()
        self.assert_has_proper_parent_cell()
        self.assert_has_proper_topology()
        self._assert_is_properly_deployed()

    def delete(self) -> None:
        if self.__parent_cell.is_deployed():
            raise CannotBeDeletedException(f"{self} cannot be deleted while its parent cell {self.__parent_cell} "
                                           f"is deployed.")
        self.__parent_cell._remove_internal_connection(self)    # May raise exception
        try:
            self.__source._remove_outgoing_connection(self)     # Access to protected member on purpose  # May raise exception
        except Exception as e:
            self.__parent_cell._add_internal_connection(self)   # Rollback
            raise e
        try:
            self.__target._remove_incoming_connection(self)     # Access to protected member on purpose  # May raise exception
        except Exception as e:
            self.__source._add_outgoing_connection(self)        # Rollback
            self.__parent_cell._add_internal_connection(self)   # Rollback
            raise e
        self.__parent_cell = None   # type: ignore
        self.__source = None        # type: ignore
        self.__target = None        # type: ignore

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.get_source()}, {self.get_target()})"


def _is_intra_cell_connection(source: "IConnectionExitPoint[T]", target: "IConnectionEntryPoint[T]") -> bool:
    """
    Check whether a connection between these endpoint would be an intra-cell connection.

    Intra-cell connection = connection between a composite cell's IO and one of its child cells"""
    source_cell = source.get_cell()
    source_parent_cell = source_cell.get_parent_cell()
    target_cell = target.get_cell()
    target_parent_cell = target_cell.get_parent_cell()
    if source_cell == target_parent_cell:
        return True
    elif target_cell == source_parent_cell:
        return True
    elif source_cell == target_cell and isinstance(source, IInput) and isinstance(target, IOutput):
        # Internal connection inside a scalablecell, directly from InputPort to OutputPort
        return True
    return False


def _is_inter_cell_connection(source: "IConnectionExitPoint[T]", target: "IConnectionEntryPoint[T]") -> bool:
    """
    Check whether a connection between these endpoints would be an inter-cell connection.

    Inter-cell connection = connection between 2 cells with the same parent cell"""
    source_cell = source.get_cell()
    target_cell = target.get_cell()
    if source_cell == target_cell and isinstance(source, IInput) and isinstance(target, IOutput):
        # Internal connection inside a scalablecell, directly from InputPort to OutputPort
        return False
    elif source_cell.get_parent_cell() == target_cell.get_parent_cell():
        return True
    return False


def _get_parent_cell(source: "IConnectionExitPoint[T]",
                     target: "IConnectionEntryPoint[T]") -> "ICompositeCell":
    """
    If a connection is made between these endpoints, what would be its parent cell?

    May raise:
     - InvalidInputException
    """
    source_cell = source.get_cell()
    source_parent_cell = source_cell.get_parent_cell()
    target_cell = target.get_cell()
    target_parent_cell = target_cell.get_parent_cell()
    if source_cell == target_parent_cell:       # Intra-cell
        if isinstance(source, pypipeline.cellio.IInput) and isinstance(target, pypipeline.cellio.IInput):
            parent_cell: "ICompositeCell" = target_parent_cell
        else:
            raise InvalidInputException(f"Illegal intra-cell connection. An intra-cell connection must depart from "
                                        f"an InputPort or arrive at an OutputPort. ")
    elif source_parent_cell == target_cell:     # Intra-cell
        if isinstance(source, pypipeline.cellio.IOutput) and isinstance(target, pypipeline.cellio.IOutput):
            parent_cell = source_parent_cell
        else:
            raise InvalidInputException(f"Illegal intra-cell connection. An intra-cell connection must depart from "
                                        f"an InputPort or arrive at an OutputPort. ")
    elif source_cell == target_cell:
        if isinstance(source, pypipeline.cellio.IInput) and isinstance(target, pypipeline.cellio.IOutput):
            assert isinstance(source_cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell)
            # Intra-cell, connecting an input port immediately with an output port
            parent_cell = source_cell
        elif isinstance(source, pypipeline.cellio.IOutput) and isinstance(target, pypipeline.cellio.IInput):
            assert source_parent_cell == target_parent_cell
            # Inter-cell, recurrent
            if source_parent_cell is not None:
                parent_cell = source_parent_cell
            else:
                raise InvalidInputException(f"A Connection can't have None as parent cell. Each connection must be "
                                            f"created inside a cell or pipeline. Are you trying to connect cells that "
                                            f"are not nested inside a pipeline?")
        else:
            raise InvalidInputException(f"A connection between inputs of the same cell or outputs of the same cell"
                                        f"is not allowed: {source} and {target}")
    elif source_parent_cell == target_parent_cell:      # Inter-cell
        if source_parent_cell is not None:
            parent_cell = source_parent_cell
        else:
            raise InvalidInputException(f"A Connection can't have None as parent cell. Each connection must be "
                                        f"created inside a cell or pipeline. Are you trying to connect cells that "
                                        f"are not nested inside a pipeline?")
    else:
        raise InvalidInputException(f"The parent cell of the connection between given source and target could not "
                                    f"be determined. You probably try to make a connection that crosses cell borders, "
                                    f"which is not allowed. ")
    assert parent_cell is not None
    return parent_cell
