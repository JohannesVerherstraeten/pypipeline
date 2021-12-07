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

"""
TODO the functions in this file are nowhere used anymore. -> Remove them?
"""

from typing import TYPE_CHECKING, Set, Any, Tuple, Collection

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline.connection import IConnection
    from pypipeline.cellio import IConnectionExitPoint, IConnectionEntryPoint


def get_downstream_neighbours(cell: "ICell",
                              recurrently: bool = True) -> "Tuple[Collection[ICell], Collection[IConnection]]":
    """
    Get cells that are directly connected to an output of this cell.

    Args:
        cell: the cells to get the downstream neighbours from.
        recurrently: include (upstream) neighbours that can be reached using an outgoing recurrent connection or not.
    Returns:
        A collection with the downstream neighbour cells, next to a collection of all connections to them.
    Raises:
        IndeterminableTopologyException
    """
    result_cells: Set["ICell"] = set()
    result_connections: Set["IConnection"] = set()
    for output in cell.get_outputs():
        outgoing_connections = output.get_outgoing_connections()
        for connection in outgoing_connections:
            target: "IConnectionEntryPoint[Any]" = connection.get_target()
            if not connection.is_inter_cell_connection():
                continue
            if not recurrently and connection.is_recurrent():
                continue
            result_cells.add(target.get_cell())
            result_connections.add(connection)
    return result_cells, result_connections


def get_upstream_neighbours(cell: "ICell",
                            recurrently: bool = True) -> "Tuple[Collection[ICell], Collection[IConnection]]":
    """
    Get cells that are directly connected to an input of this cell.

    Args:
        cell: the cells to get the upstream neighbours from.
        recurrently: include (downstream) neighbours that can be reached using an incoming recurrent connection or not.
    Returns:
        A collection with the upstream neighbour cells, next to a collection of all connections from them.
    Raises:
        IndeterminableTopologyException
    """
    result_cells: Set["ICell"] = set()
    result_connections: Set["IConnection"] = set()
    for input_ in cell.get_inputs():
        incoming_connections = input_.get_incoming_connections()
        for connection in incoming_connections:
            source: "IConnectionExitPoint[Any]" = connection.get_source()
            if not connection.is_inter_cell_connection():
                continue
            if not recurrently and connection.is_recurrent():
                continue
            result_cells.add(source.get_cell())
            result_connections.add(connection)
    return result_cells, result_connections


def get_all_downstream_cells(cell: "ICell",
                             recurrently: bool = True) -> "Tuple[Collection[ICell], Collection[IConnection]]":
    """
    Get all cells that can be reached when continuously following all connections downstream, starting from the given
    cell.

    Args:
        cell: the cells to get all downstream cells from.
        recurrently: include (upstream) neighbours that can be reached using an outgoing recurrent connection or not.
    Returns:
        A collection with all downstream cells, next to a collection of all connections to them.
    Raises:
        IndeterminableTopologyException
    """
    result_cells: Set["ICell"] = set()
    neighbour_cells, neighbour_connections = get_downstream_neighbours(cell, recurrently)
    result_connections: Set["IConnection"] = set(neighbour_connections)
    cells_to_check: Set["ICell"] = set(neighbour_cells)
    while len(cells_to_check) > 0:
        cell_to_check = cells_to_check.pop()
        result_cells.add(cell_to_check)
        neighbour_cells, neighbour_connections = get_downstream_neighbours(cell_to_check, recurrently)
        result_connections.update(neighbour_connections)
        neighbours_to_check = set(neighbour_cells).difference(result_cells)
        cells_to_check.update(neighbours_to_check)
    return result_cells, result_connections


def get_all_upstream_cells(cell: "ICell",
                           recurrently: bool = True) -> "Tuple[Collection[ICell], Collection[IConnection]]":
    """
    Get all cells that can be reached when continuously following all connections upstream, starting from the given
    cell.

    Args:
        cell: the cells to get all upstream cells from.
        recurrently: include (downstream) neighbours that can be reached using an incoming recurrent connection or not.
    Returns:
        A collection with all upstream cells, next to a collection of all connections from them.
    Raises:
        IndeterminableTopologyException
    """
    result_cells: Set["ICell"] = set()
    neighbour_cells, neighbour_connections = get_upstream_neighbours(cell, recurrently)
    result_connections: Set["IConnection"] = set(neighbour_connections)
    cells_to_check: Set["ICell"] = set(neighbour_cells)
    while len(cells_to_check) > 0:
        cell_to_check = cells_to_check.pop()
        result_cells.add(cell_to_check)
        neighbour_cells, neighbour_connections = get_upstream_neighbours(cell_to_check, recurrently)
        result_connections.update(neighbour_connections)
        neighbours_to_check = set(neighbour_cells).difference(result_cells)
        cells_to_check.update(neighbours_to_check)
    return result_cells, result_connections
