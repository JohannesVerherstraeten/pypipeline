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

from typing import Optional, List

from pypipeline.cell.compositecell.acompositecell import ACompositeCell
from pypipeline.cell.compositecell.icompositecell import ICompositeCell


class Pipeline(ACompositeCell):

    def __init__(self, parent_cell: "Optional[ICompositeCell]", name: str, max_nb_internal_cells: int = 99999):
        super(Pipeline, self).__init__(parent_cell, name=name, max_nb_internal_cells=max_nb_internal_cells)

    def _on_pull(self) -> None:
        """
        Override this method to add functionality that must happen when pulling the cell.

        During a pull, a cell must pull its inputs, execute it's functionality and set its outputs.

        Raises:
            Exception: any exception that the user may raise when overriding _on_pull.

        Won't raise:
            NotDeployedException: this method will only be called when the cell is already deployed.
            IndeterminableTopologyException: this method will only be called when the cell is already deployed.
        """
        # Pull all sink cells
        sink_cells = [cell for cell in self.get_internal_cells() if cell.is_sink_cell()]
        for sink_cell in sink_cells:
            sink_cell.pull()

        # Set the outputs of this pipeline
        for output in self.get_outputs():
            output_incoming_connections = output.get_incoming_connections()
            assert len(output_incoming_connections) == 1
            output.set_value(output_incoming_connections[0].pull())

    def get_nb_available_pulls(self) -> Optional[int]:
        """
        Returns the total number of times this cell can be pulled.

        Default implementation here is only valid if the cell pulls all its inputs once per cell pull. If this is
        not the case, the cell must override this method. Also when a cell has no inputs, it has to override this
        method itself.

        Returns:
            The total number of times this cell can be pulled, or
            None if cell can be pulled infinitely or an unspecified amount of time.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        sink_cells = [cell for cell in self.get_internal_cells() if cell.is_sink_cell()]
        if len(sink_cells) == 0:
            return None
        sink_cell_pulls: List[Optional[int]] = [sink_cell.get_nb_available_pulls() for sink_cell in sink_cells]
        sink_cell_pulls_no_none: List[int] = [val for val in sink_cell_pulls if val is not None]
        if len(sink_cell_pulls_no_none) == 0:
            return None
        else:
            return min(sink_cell_pulls_no_none)
