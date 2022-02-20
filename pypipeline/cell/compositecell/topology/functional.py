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

from typing import Optional, List, Set, TYPE_CHECKING, Sequence, Iterable, Tuple, Dict

import pypipeline
from pypipeline.cell.icell import ICell
from pypipeline.exceptions import IndeterminableTopologyException, InvalidInputException, InvalidStateException
from pypipeline.cell.compositecell.topology.descriptive import get_all_downstream_cells, get_all_upstream_cells
from pypipeline.validation import BoolExplained, FalseExplained, TrueExplained, raise_if_not


if TYPE_CHECKING:
    from pypipeline.cell.compositecell.icompositecell import ICompositeCell
    from pypipeline.connection import IConnection


class InvalidTopologyException(Exception):
    """
    Raised if a Topology is created with a cell ordering that could be valid, but doesn't match with the user's
    explicit recurrency markings.
    """
    pass


class Topology(object):
    """
    A Topology object represents the topology of a composite cell's internal cells. It maintains the information
    about which internal cells are sources and sinks, and which internal connections are recurrent or not.

    A topology is fully determined by the ordering in which the internal cells must be executed.

    Note that multiple valid equivalent topologies can exist for a composite cell's internal cells. Ex:
          -> c2 -
        /        \
    c1 -           -> c4
        \        /
          -> c3 -
    Both following orderings are valid and equivalent, as an executing in both orderings would result in the same
    computations and result.
     - c1, c2, c3, c4
     - c1, c3, c2, c4
    Equivalence of topologies can be checked with `topology1.is_equivalent_with(topology2)`.
    """

    def __init__(self, parent_cell: "ICompositeCell", ordered_cells: List["ICell"]):
        """
        Args:
            parent_cell: the composite cell to which this Topology belongs.
            ordered_cells: the ordering of the internal cells that determine the topology.
        """
        raise_if_not(self.can_have_as_parent_cell(parent_cell), InvalidInputException)
        raise_if_not(self.can_have_as_ordered_cells(parent_cell, ordered_cells), InvalidInputException)

        recurrent_connection_map = _find_recurrent_connections(parent_cell, ordered_cells)
        raise_if_not(self.can_have_as_recurrent_connection_map(parent_cell, recurrent_connection_map),
                     InvalidTopologyException)
        self.__parent_cell = parent_cell
        self.__ordered_cells = ordered_cells
        self.__connection_is_recurrent: Dict["IConnection", bool] = recurrent_connection_map
        self.__source_cells: Set[ICell] = _find_source_cells(ordered_cells)
        self.__sink_cells: Set[ICell] = _find_sink_cells(ordered_cells)

    def get_parent_cell(self) -> "ICompositeCell":
        """
        Returns:
            The composite cell to which this Topology belongs.
        """
        return self.__parent_cell

    @classmethod
    def can_have_as_parent_cell(cls, parent_cell: "ICompositeCell") -> BoolExplained:
        """
        Args:
            parent_cell: the cell to validate.
        Returns:
            TrueExplained if the given cell is a valid parent cell for this topology. FalseExplained otherwise.
        """
        if not isinstance(parent_cell, pypipeline.cell.compositecell.icompositecell.ICompositeCell):
            return FalseExplained(f"The parent cell of a Topology should be a CompositeCell, "
                                  f"got {type(parent_cell)}. ")
        return TrueExplained()

    def get_ordered_cells(self) -> List[ICell]:
        """
        Returns:
            The internal cells of this topology, in topological ordering.
        """
        return self.__ordered_cells

    @classmethod
    def can_have_as_ordered_cells(cls, parent_cell: "ICompositeCell", ordered_cells: List["ICell"]) -> BoolExplained:
        """
        Args:
            parent_cell: a parent cell for the topological ordering.
            ordered_cells: an ordering of the internal cells of the parent cell.
        Returns:
            TrueExplained if the given internal cell ordering is a valid ordering given the parent cell.
            FalseExplained otherwise.
        """
        if not isinstance(ordered_cells, list):
            return FalseExplained(f"The ordered cells of a Topology should be a list of cells, "
                                  f"got {type(ordered_cells)}. ")

        for cell in ordered_cells:
            if not isinstance(cell, pypipeline.cell.icell.ICell):
                return FalseExplained(f"The ordered cells of a Topology should be a list of cells, "
                                      f"got an object of type {type(cell)} in the list. ")
            if not cell.get_parent_cell() == parent_cell:
                return FalseExplained(f"At least one of the ordered cells has a different parent cell than the "
                                      f"others: {cell}")

        unordered_cells = set(parent_cell.get_internal_cells()).difference(set(ordered_cells))
        if len(unordered_cells) > 0:
            return FalseExplained(f"Not all cells of parent cell {parent_cell} are in the ordered cells list: "
                                  f"{unordered_cells}")

        return TrueExplained()

    @classmethod
    def can_have_as_recurrent_connection_map(cls,
                                             parent_cell: "ICompositeCell",
                                             connections_map: Dict["IConnection", bool]) -> BoolExplained:
        """
        Args:
            parent_cell: a parent cell for the topological ordering.
            connections_map: a dictionary that maps every internal connection of the parent cell to a boolean,
                representing whether the connection is recurrent or not.
        Returns:
            TrueExplained if the given connection map is a valid connection map given the parent cell.
            FalseExplained otherwise.
        """
        if not isinstance(connections_map, dict):
            return FalseExplained(f"The recurrent connections map of a Topology should be a dictionary mapping "
                                  f"connections to a boolean value that indicates whether they are recurrent or not, "
                                  f"got {type(connections_map)}. ")

        for connection, is_recurrent in connections_map.items():
            if not isinstance(connection, pypipeline.connection.iconnection.IConnection):
                return FalseExplained(f"The recurrent connection map should contain IConnection objects as keys, "
                                      f"but got an object of type {type(connection)}. ")
            if not connection.get_parent_cell() == parent_cell:
                return FalseExplained(f"At least one of the connections has a different parent cell than the "
                                      f"others: {connection}")
            if not isinstance(is_recurrent, bool):
                return FalseExplained(f"The recurrent connection map should contain booleans as values, "
                                      f"but got an object of type {type(is_recurrent)}. ")

            # Check if the connections are valid given the explicit recurrency markings of the user
            if connection.is_explicitly_marked_as_recurrent() and not connections_map[connection]:
                return FalseExplained(f"{connection} is explicitly marked as recurrent by the user, but is not "
                                      f"recurrent according to the connection map.")
            if connection.is_explicitly_marked_as_non_recurrent() and connections_map[connection]:
                return FalseExplained(f"{connection} is explicitly marked as non-recurrent by the user, but is "
                                      f"recurrent according to the connection map.")

        unmapped_connections = set(parent_cell.get_internal_connections()).difference(set(connections_map))
        if len(unmapped_connections) > 0:
            return FalseExplained(f"Not all connections of parent cell {parent_cell} are in the connection "
                                  f"map: {unmapped_connections}")

        return TrueExplained()

    def _get_connection_recurrency_mapping(self) -> Dict["IConnection", bool]:
        return self.__connection_is_recurrent

    def get_recurrent_connections(self) -> Set["IConnection"]:
        """
        Returns:
            All connections that are recurrent according to this topology.
        """
        return {c for c, is_recurrent in self.__connection_is_recurrent.items() if is_recurrent}

    def has_as_recurrent_connection(self, connection: "IConnection") -> bool:
        """
        Args:
            connection: an internal connection of this topology.
        Returns:
            True if the given connection is a recurrent one according to this topology. False otherwise.
        """
        assert connection in self.__connection_is_recurrent
        return self.__connection_is_recurrent[connection]

    def get_source_cells(self) -> Set[ICell]:
        """
        Returns:
            All cells that are source cells according to this topology.
        """
        return self.__source_cells

    def has_as_source_cell(self, cell: ICell) -> bool:
        """
        Args:
            cell: an internal cell of this topology.
        Returns:
            True if the given internal cell is a source cell according to this topology. False otherwise.
        """
        assert cell in self.__ordered_cells
        return cell in self.__source_cells

    def get_sink_cells(self) -> Set[ICell]:
        """
        Returns:
            All cells that are sink cells according to this topology.
        """
        return self.__sink_cells

    def has_as_sink_cell(self, cell: ICell) -> bool:
        """
        Args:
            cell: an internal cell of this topology.
        Returns:
            True if the given internal cell is a sink cell according to this topology. False otherwise.
        """
        assert cell in self.__ordered_cells
        return cell in self.__sink_cells

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if this topology is invalid.
        """
        raise_if_not(self.can_have_as_parent_cell(self.get_parent_cell()), InvalidStateException)
        raise_if_not(self.can_have_as_ordered_cells(self.get_parent_cell(), self.get_ordered_cells()),
                     InvalidStateException)
        raise_if_not(self.can_have_as_recurrent_connection_map(self.get_parent_cell(),
                                                               self._get_connection_recurrency_mapping()),
                     InvalidStateException)

    def is_equivalent_with(self, other: "Topology") -> Tuple[bool, List["IConnection"]]:
        """
        Two topologies are equivalent if they both consider the same connections as recurrent.

        The second return value contains the connections that are considered differently by the two topologies.
        """
        assert self.get_parent_cell() == other.get_parent_cell()

        my_connections = self._get_connection_recurrency_mapping()
        other_connections = other._get_connection_recurrency_mapping()
        if my_connections != other_connections:
            my_recurrent_connections = {c for c, is_rec in my_connections.items() if is_rec}
            other_recurrent_connections = {c for c, is_rec in other_connections.items() if is_rec}
            uncertain_connections = my_recurrent_connections.symmetric_difference(other_recurrent_connections)
            return False, list(uncertain_connections)
        return True, []

    def delete(self) -> None:
        """Should only be called by the destructor of a CompositeCell."""
        self.__parent_cell = None               # type: ignore
        self.__ordered_cells = None             # type: ignore
        self.__connection_is_recurrent = None   # type: ignore
        self.__source_cells = None              # type: ignore
        self.__sink_cells = None                # type: ignore

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Topology):
            return False
        if self.get_parent_cell() != other.get_parent_cell():
            return False
        if self.get_ordered_cells() != other.get_ordered_cells():
            return False
        if self._get_connection_recurrency_mapping() != other._get_connection_recurrency_mapping():
            return False
        return True

    def __str__(self) -> str:
        return f"Topology({self.get_parent_cell()}, {[str(cell) for cell in self.get_ordered_cells()]})"


def select_a_topology(cell: "ICompositeCell") -> Topology:
    """
    Searches for all possible topologies of the given cell, and selects one of them if they're all equivalent.

    If no topology or multiple non-equivalent topologies are possible, an IndeterminableTopologyException
    is raised.

    Args:
        cell: the composite cell to select a topology for.
    Returns:
        The selected topology.
    Raises:
        IndeterminableTopologyException: if no topology is possible or multiple non-equivalent topologies are possible,
        an IndeterminableTopologyException is raised.

    """
    all_topologies = find_all_topologies(cell)

    # Check whether there is a topological ordering of the internal cells possible.
    if len(all_topologies) == 0:
        message = f"{str(cell)}: Could not determine a topological order. \n" \
                  f"This is probably caused by a recurrent connection which has no initial_value at \n" \
                  f"its start. " \
                  f"Make sure you provide an initial value `Output(self, 'output', initial_value=<value>)` \n" \
                  f"when creating an output that might provide an input for a next time step. "
        raise IndeterminableTopologyException(message)

    topology_0 = all_topologies[0]

    # Check whether all possible topological orderings are equivalent.
    all_uncertain_connections: Set["IConnection"] = set()
    for topoloy_i in all_topologies[1:]:
        is_equivalent, uncertain_connections = topoloy_i.is_equivalent_with(topology_0)
        all_uncertain_connections.update(uncertain_connections)
    if len(all_uncertain_connections) > 0:
        uncertain_connections_pretty = '\n'.join([' * ' + str(conn) for conn in all_uncertain_connections])
        message = f"{str(cell)}: Multiple topological orderings of the internal cells are possible, \n" \
                  f"of which some have different recurrent connections. So the ordering in which the internal " \
                  f"cells must be executed is undetermined. \n" \
                  f"Connections which can be interpreted both recurrent/not-recurrent are: \n" \
                  f"{uncertain_connections_pretty}. \n" \
                  f"Try adding this parameter to the connection: " \
                  f"Connection(source, target, explicitly_mark_as_recurrent=<bool>)"
        raise IndeterminableTopologyException(message)

    return topology_0


def find_all_topologies(cell: "ICompositeCell") -> List[Topology]:
    """
    Searches for all possible topologies of the given cell.

    Args:
        cell: the composite cell to find all possible topologies for.
    Returns:
        All possible topologies for the given composite cell.
    """
    result: List["Topology"] = []
    for ordering in _find_all_topological_orders(cell):
        try:
            topology = Topology(cell, ordering)
        except InvalidTopologyException:
            # Invalid topology when taking the explicit recurrency markings of the user into account.
            continue
        else:
            result.append(topology)
    return result


def _find_all_topological_orders(cell: "ICompositeCell",
                                 path: Sequence[ICell] = (),
                                 discovered: Iterable[ICell] = (),
                                 hidden_connections: Optional[Set["IConnection"]] = None) -> List[List[ICell]]:
    """
    Searches for all possible topological orderings of the composite cell's internal cells.

    Note that this function doesn't take the explicit recurrency markings of the user into account.

    Implements Kahn's algorithm.
    Based on https://www.techiedelight.com/find-all-possible-topological-orderings-of-dag/

    Note that not all topological orders are necessarily valid according to the explicit recurrency markings
    that the user made when creating the connections. See find_all_topologies for filtering out the invalid ones.
    """
    path = list(path)
    discovered = set(discovered)
    if hidden_connections is None:
        hidden_connections = set([conn for conn in cell.get_internal_connections() if conn.is_intra_cell_connection()])

    if len(path) == len(cell.get_internal_cells()):
        return [path]

    paths = []
    for internal_cell in cell.get_internal_cells():

        if internal_cell in discovered or not _might_be_a_source_cell(internal_cell, hidden_connections):
            continue

        hidden_connections_clone = hidden_connections.copy()
        for output in internal_cell.get_outputs():
            hidden_connections_clone.update(output.get_outgoing_connections())
        for input_ in internal_cell.get_inputs():
            hidden_connections_clone.update(input_.get_incoming_connections())

        path.append(internal_cell)
        discovered.add(internal_cell)

        paths.extend(_find_all_topological_orders(cell, path.copy(), discovered, hidden_connections_clone))

        path.pop()
        discovered.remove(internal_cell)

    return paths


def _might_be_a_source_cell(cell: ICell, hidden_connections: Optional[Set["IConnection"]] = None) -> bool:
    cell_inputs = cell.get_inputs()

    # If the cell has no inputs, it is a source cell.
    if len(cell_inputs) == 0:
        return True

    # If the cell has inputs, it can act as a source cell if all it's incoming connections satisfy
    # at least one of the following constraints:
    # - the incoming connection departs at an input that belongs to the parent cell
    #   (happens in case of connections from a pipeline cell input to its subcell's inputs for example)
    # - the incoming connection departs at an output which has a default value provided
    all_incoming_connections_satisfy = True
    for cell_input in cell_inputs:
        incoming_connections = cell_input.get_incoming_connections()
        for incoming_connection in incoming_connections:
            if hidden_connections is not None and incoming_connection in hidden_connections:
                continue
            source = incoming_connection.get_source()
            if source.get_cell() == cell.get_parent_cell():
                continue
            elif source.has_initial_value() and not incoming_connection.is_explicitly_marked_as_non_recurrent():
                continue
            else:
                all_incoming_connections_satisfy = False
                break
        if not all_incoming_connections_satisfy:
            break
    return all_incoming_connections_satisfy


def _might_be_a_sink_cell(cell: ICell) -> bool:
    cell_outputs = cell.get_outputs()

    # If the cell has no outputs, it is a sink cell.
    if len(cell_outputs) == 0:
        return True

    # If the cell has outputs, it only can act as a sink cell if all it's outgoing connections satisfy
    # at least one of the following constraints:
    # - the outgoing connection arrives at an output that belongs to the parent cell
    #   (happens in case of connections from a subcells output to its parent cell output for example)
    # - the outgoing connection has a default value provided
    all_outgoing_connections_satisfy = True
    for cell_output in cell_outputs:
        outgoing_connections = cell_output.get_outgoing_connections()
        for outgoing_connection in outgoing_connections:
            source = outgoing_connection.get_source()
            target = outgoing_connection.get_target()
            if target.get_cell() == cell.get_parent_cell():
                continue
            elif source.has_initial_value() and not outgoing_connection.is_explicitly_marked_as_non_recurrent():
                continue
            else:
                all_outgoing_connections_satisfy = False
                break
        if not all_outgoing_connections_satisfy:
            break
    return all_outgoing_connections_satisfy


def _find_source_cells(ordered_cells: List[ICell]) -> Set[ICell]:
    result: Set[ICell] = set()
    visited: Set[ICell] = set()
    for cell in ordered_cells:
        if cell in visited:
            continue
        result.add(cell)
        downstream_cells, _ = get_all_downstream_cells(cell, recurrently=True)
        visited.update(downstream_cells)

    return result


def _find_sink_cells(ordered_cells: List["ICell"]) -> Set[ICell]:
    result: Set[ICell] = set()
    visited: Set[ICell] = set()
    for cell in reversed(ordered_cells):
        if cell in visited:
            continue
        result.add(cell)
        upstream_cells, _ = get_all_upstream_cells(cell, recurrently=True)
        visited.update(upstream_cells)

    return result


def _find_recurrent_connections(parent_cell: "ICompositeCell", ordered_cells: List[ICell]) -> Dict["IConnection", bool]:
    result: Dict["IConnection", bool] = dict()
    for internal_connection in parent_cell.get_internal_connections():
        if internal_connection.is_intra_cell_connection():
            result[internal_connection] = False     # never recurrent
            continue
        source_cell = internal_connection.get_source().get_cell()
        source_cell_idx = ordered_cells.index(source_cell)
        target_cell = internal_connection.get_target().get_cell()
        target_cell_idx = ordered_cells.index(target_cell)

        result[internal_connection] = source_cell_idx >= target_cell_idx    # recurrent in this case

    return result
