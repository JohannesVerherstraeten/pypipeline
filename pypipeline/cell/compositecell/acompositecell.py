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

from typing import Optional, Dict, Set, TYPE_CHECKING, Any, List, Sequence

import pypipeline
from pypipeline.cell.acell import ACell
from pypipeline.cell.compositecell.icompositecell import ICompositeCell
from pypipeline.cell.compositecell.topology import Topology, select_a_topology
from pypipeline.connection import Connection
from pypipeline.exceptions import IndeterminableTopologyException, InvalidStateException, InvalidInputException, \
    AlreadyDeployedException, CannotBeDeletedException
from pypipeline.validation import BoolExplained, raise_if_not, TrueExplained, FalseExplained

if TYPE_CHECKING:
    from pypipeline.cell.icellobserver import Event, IObservable
    from pypipeline.cell.icell import ICell
    from pypipeline.cellio import IO, IInput, IOutput
    from pypipeline.connection import IConnection


class ACompositeCell(ACell, ICompositeCell):
    """
    Abstract composite cell class.

    An ICell is owned by its (optional) parent cell.
    An ICell owns its IO.
    An ICompositeCell owns its internal cells.
    An ICompositeCell owns its internal connections.

    An ICell is the controlled class in the ICompositeCell-ICell relation, as internal cell of a parent composite class.
    An ICompositeCell is the controlling class in the ICompositeCell-ICell relation, as parent of internal cells.

    An ICell is the controlled class in the IObserver-IObservable relation, as observable.
    An ICompositeCell is the controlling class in the IObserver-IObservable relation, as potential observer of internal cells.

    An ICell is the controlled class in the IO-ICell relation, as owner of the IO.

    An ICompositeCell is the controlled class in the IConnection-ICompositeCell relation, as the parent cell of the
    connection.
    """

    __INTERNAL_CELLS_KEY: str = "internal_cells"
    __INTERNAL_CONNECTIONS_KEY: str = "internal_connections"

    __CONNECTION_SOURCE_NAME_KEY: str = "connection_source_name"
    __CONNECTION_TARGET_NAME_KEY: str = "connection_target_name"
    __CONNECTION_SOURCE_CELL_KEY: str = "connection_source_cell"
    __CONNECTION_TARGET_CELL_KEY: str = "connection_target_cell"

    def __init__(self,
                 parent_cell: "Optional[ICompositeCell]",
                 name: str,
                 max_nb_internal_cells: int = 99999,
                 wants_to_observe_its_internal_cells: bool = True):
        """
        Args:
            parent_cell: the cell in which this cell must be nested.
            name: name of this cell.
            max_nb_internal_cells: the max amount of internal cells that this composite cell can support.
            wants_to_observe_its_internal_cells: if True, this cell will add itself as an observer when new
                internal cells register.
        Raises:
            InvalidInputException
        """
        super(ACompositeCell, self).__init__(parent_cell, name=name)
        self.__max_nb_internal_cells = max_nb_internal_cells
        self.__wants_to_observe_its_internal_cells = wants_to_observe_its_internal_cells

        self.__internal_cells: "Dict[str, ICell]" = dict()
        self.__internal_connections: List["IConnection"] = []
        self.__internal_topology: Optional[Topology] = None

    # ------ IO ------

    def get_inputs_recursively(self) -> Sequence["IInput"]:
        result: List["IInput"] = []
        result.extend(self.get_inputs())
        for cell in self.get_internal_cells():
            result.extend(cell.get_inputs_recursively())
        return result

    def get_input_recursively(self, full_name: str) -> "IInput":
        if "." not in full_name:
            return self.get_input(full_name)
        cell_name, io_name = full_name.split(".", maxsplit=1)
        cell = self.get_internal_cell_with_name(cell_name)      # TODO may raise exceptions
        return cell.get_input_recursively(io_name)

    def get_outputs_recursively(self) -> Sequence["IOutput"]:
        result: List["IOutput"] = []
        result.extend(self.get_outputs())
        for cell in self.get_internal_cells():
            result.extend(cell.get_outputs_recursively())
        return result

    def get_output_recursively(self, full_name: str) -> "IOutput":
        if "." not in full_name:
            return self.get_output(full_name)
        cell_name, io_name = full_name.split(".", maxsplit=1)
        cell = self.get_internal_cell_with_name(cell_name)       # TODO may raise exceptions
        return cell.get_output_recursively(io_name)

    def _add_io(self, io: "IO") -> None:
        if io.get_name() in self.get_internal_cell_names():
            raise InvalidInputException(f"{self} already has a cell with name {io.get_name()}")
        super(ACompositeCell, self)._add_io(io)

    def assert_has_proper_io(self) -> None:
        super(ACompositeCell, self).assert_has_proper_io()
        internal_cell_names = self.get_internal_cell_names()
        for io in self.get_all_io():
            if io.get_name() in internal_cell_names:
                raise InvalidStateException(f"{self} has an internal cell with the same name as an io: "
                                            f"{io.get_name()}")

    # ------ Internal cells (A composite cell can be an observer of its internal cells)------

    def has_as_observable(self, observable: "IObservable") -> bool:
        if not self._wants_to_observe_its_internal_cells():
            return False
        if not isinstance(observable, pypipeline.cell.ICell):
            return False
        return self.has_as_internal_cell(observable)

    def update(self, event: "Event") -> None:
        # Called when the state of an internal cell changes.
        self.notify_observers(event)

    def get_internal_cells(self) -> "Sequence[ICell]":
        return tuple(self.__internal_cells.values())

    def get_internal_cells_recursively(self) -> Sequence["ICell"]:
        result = []
        for cell in self.get_internal_cells():
            result.append(cell)
            if isinstance(cell, ICompositeCell):
                result.extend(cell.get_internal_cells_recursively())
        return result

    def get_internal_cell_names(self) -> Sequence[str]:
        return tuple(self.__internal_cells.keys())

    def get_internal_cell_with_name(self, name: str) -> "ICell":
        return self.__internal_cells[name]

    def _add_internal_cell(self, cell: "ICell") -> None:
        self._clear_internal_topology()     # May raise AlreadyDeployedException
        raise_if_not(self.can_have_as_internal_cell(cell), InvalidInputException)
        raise_if_not(self.can_have_as_nb_internal_cells(self.get_nb_internal_cells() + 1), InvalidInputException)
        if self.has_as_internal_cell(cell):
            raise InvalidInputException(f"{self} already has cell {cell} as internal cell.")
        if cell.get_name() in self.get_internal_cell_names():
            raise InvalidInputException(f"{self} already has a cell with name {cell.get_name()}.")
        if cell.get_name() in self.get_io_names():
            raise InvalidInputException(f"{self} already has an IO with name {cell.get_name()}.")

        if self._wants_to_observe_its_internal_cells():
            cell._add_observer(self)            # May raise exceptions

        try:
            cell._set_parent_cell(self)         # May raise exceptions
        except Exception as e:
            if self._wants_to_observe_its_internal_cells():
                cell._remove_observer(self)     # Rollback
            raise e

        self.__internal_cells[cell.get_name()] = cell

    def _remove_internal_cell(self, cell: "ICell") -> None:
        self._clear_internal_topology()     # May raise AlreadyDeployedException
        if not self.has_as_internal_cell(cell):
            raise InvalidInputException(f"{self} doesn't have {cell} as an internal cell.")

        cell._set_parent_cell(None)             # May raise exceptions

        if self._wants_to_observe_its_internal_cells():
            try:
                cell._remove_observer(self)     # May raise exceptions
            except Exception as e:
                cell._set_parent_cell(self)     # Rollback
                raise e

        del self.__internal_cells[cell.get_name()]

    def can_have_as_internal_cell(self, cell: "ICell") -> BoolExplained:
        # Can I have the cell as internal cell and observable?
        if not isinstance(cell, pypipeline.cell.icell.ICell):
            return FalseExplained(f"{self}: internal cell should be of type ICell, got {type(cell)}. ")
        return TrueExplained()

    def get_nb_internal_cells(self) -> int:
        return len(self.__internal_cells)

    def get_max_nb_internal_cells(self) -> int:
        return self.__max_nb_internal_cells

    def can_have_as_nb_internal_cells(self, number_of_internal_cells: int) -> BoolExplained:
        max_nb = self.get_max_nb_internal_cells()
        if not 0 <= number_of_internal_cells <= max_nb:
            return FalseExplained(f"{self}: the number of internal cells should be in range 0..{max_nb} (inclusive)")
        return TrueExplained()

    def has_as_internal_cell(self, cell: "ICell") -> bool:
        return cell in self.__internal_cells.values()

    def has_as_internal_cell_name(self, name: str) -> bool:
        return name in self.__internal_cells

    def assert_has_proper_internal_cells(self) -> None:
        raise_if_not(self.can_have_as_nb_internal_cells(self.get_nb_internal_cells()), InvalidStateException)

        internal_cells = self.get_internal_cells()
        internal_cell_names = self.get_internal_cell_names()
        io_names = self.get_io_names()
        for cell in self.get_internal_cells():
            raise_if_not(self.can_have_as_internal_cell(cell), InvalidStateException)
            if internal_cells.count(cell) > 1:
                raise InvalidStateException(f"{self} has {cell} multiple times as internal cell.")
            if internal_cell_names.count(cell.get_name()) > 1:
                raise InvalidStateException(f"{self} has multiple internal cells with the same name: "
                                            f"{cell.get_name()}.")
            if cell.get_name() in io_names:
                raise InvalidStateException(f"{self} has io with the same name as an internal cell: "
                                            f"{cell.get_name()}.")
            if cell.get_parent_cell() != self:
                raise InvalidStateException(f"Inconsistent relation: {cell} doesn't have {self} as parent cell.")
            if self._wants_to_observe_its_internal_cells() and not cell.has_as_observer(self):
                raise InvalidStateException(f"Inconsistent relation: {cell} doesn't have {self} as observer.")
            cell.assert_is_valid()

    def _wants_to_observe_its_internal_cells(self) -> bool:
        """
        If a composite cell wants to observe its internal cells, it will be added as an observer when new internal
        cells register.

        Returns:
            True if this cell wants to observe its internal cells.
        """
        return self.__wants_to_observe_its_internal_cells

    # ------ Internal connections ------

    def _add_internal_connection(self, connection: "IConnection") -> None:
        self._clear_internal_topology()
        raise_if_not(self.can_have_as_internal_connection(connection), InvalidInputException)
        if self.has_as_internal_connection(connection):
            raise InvalidInputException(f"{self} already has a connection {connection}")
        self.__internal_connections.append(connection)

    def _remove_internal_connection(self, connection: "IConnection") -> None:
        self._clear_internal_topology()
        if not self.has_as_internal_connection(connection):
            raise InvalidInputException(f"{self} doesn't have a connection {connection}")
        self.__internal_connections.remove(connection)

    def can_have_as_internal_connection(self, connection: "IConnection") -> BoolExplained:
        if not isinstance(connection, pypipeline.connection.connection.IConnection):
            return FalseExplained(f"{self}: internal connection should be of type IConnection, got {type(connection)}.")
        return TrueExplained()

    def get_internal_connections(self) -> "Sequence[IConnection]":
        return tuple(self.__internal_connections)

    def has_as_internal_connection(self, connection: "IConnection") -> bool:
        return connection in self.__internal_connections

    def assert_has_proper_internal_connections(self) -> None:
        for connection in self.get_internal_connections():
            raise_if_not(self.can_have_as_internal_connection(connection), InvalidStateException)
            if connection.get_parent_cell() != self:
                raise InvalidStateException(f"Inconsistent relation: {connection} doesn't have {self} as parent cell.")

            connection.assert_is_valid()

    # ------ Deployment ------

    def is_deployable(self) -> "BoolExplained":
        if not self._internal_topology_is_set():
            try:
                self._set_internal_topology()
            except IndeterminableTopologyException as e:
                return FalseExplained(repr(e))
        result = super(ACompositeCell, self).is_deployable()
        if not result:
            return result
        for cell in self.get_internal_cells():
            result *= cell.is_deployable()
        return result

    def inputs_are_provided(self) -> "BoolExplained":
        result: BoolExplained = super(ACompositeCell, self).inputs_are_provided()
        for cell in self.get_internal_cells():
            result *= cell.inputs_are_provided()
        return result

    def _on_deploy(self) -> None:
        if not self._internal_topology_is_set():
            self._set_internal_topology()
        super(ACompositeCell, self)._on_deploy()
        try:
            for cell in self.get_internal_cells_in_topological_order():
                cell.deploy()
        except Exception as e:
            for cell in self.get_internal_cells_in_topological_order():
                if cell.is_deployed():
                    cell.undeploy()
            raise e

    def _on_undeploy(self) -> None:
        super(ACompositeCell, self)._on_undeploy()
        for cell in reversed(self.get_internal_cells_in_topological_order()):
            cell.undeploy()

    # ------ State synchronization between a cell and its clones ------

    def _get_sync_state(self) -> Dict[str, Any]:
        with self._get_pull_lock():
            state = super(ACompositeCell, self)._get_sync_state()

            # internal connections
            internal_connection_states = []
            for connection in self.get_internal_connections():
                connection_state: Dict[str, Any] = dict()
                source = connection.get_source()
                connection_state[self.__CONNECTION_SOURCE_CELL_KEY] = source.get_cell().get_name()
                connection_state[self.__CONNECTION_SOURCE_NAME_KEY] = source.get_name()
                target = connection.get_target()
                connection_state[self.__CONNECTION_TARGET_CELL_KEY] = target.get_cell().get_name()
                connection_state[self.__CONNECTION_TARGET_NAME_KEY] = target.get_name()
                internal_connection_states.append(connection_state)
            state[self.__INTERNAL_CONNECTIONS_KEY] = internal_connection_states

            # internal cells
            internal_cells = self.get_internal_cells()

            internal_cells_state = {cell.get_name(): cell._get_sync_state() for cell in internal_cells}
            state[self.__INTERNAL_CELLS_KEY] = internal_cells_state
        return state

    def _set_sync_state(self, state: Dict) -> None:     # TODO cleanup
        with self._get_pull_lock():
            super(ACompositeCell, self)._set_sync_state(state)

            assert self.__INTERNAL_CONNECTIONS_KEY in state
            assert self.__INTERNAL_CELLS_KEY in state

            # add / update internal connections
            state_dict_connections: "Set[IConnection]" = set()
            for connection_state in state[self.__INTERNAL_CONNECTIONS_KEY]:
                source_cell_name = connection_state[self.__CONNECTION_SOURCE_CELL_KEY]
                source_cell = self if source_cell_name == self.get_name() else self.get_internal_cell_with_name(source_cell_name)   # TODO may raise exceptions
                source_output_name = connection_state[self.__CONNECTION_SOURCE_NAME_KEY]
                source = source_cell.get_io(source_output_name)
                assert isinstance(source, pypipeline.cellio.icellio.IConnectionExitPoint)

                target_cell_name = connection_state[self.__CONNECTION_TARGET_CELL_KEY]
                target_cell = self if target_cell_name == self.get_name() else self.get_internal_cell_with_name(target_cell_name)   # TODO may raise exceptions
                target_input_name = connection_state[self.__CONNECTION_TARGET_NAME_KEY]
                target = target_cell.get_io(target_input_name)
                assert isinstance(target, pypipeline.cellio.icellio.IConnectionEntryPoint)

                if not source.has_outgoing_connection_to(target):
                    self.logger.info(f"Adding connection inside a clone between `{source.get_name()}` "
                                     f"and `{target.get_name()}` as part of synchronization with the proxy.")
                    conn = Connection(source, target)
                    state_dict_connections.add(conn)
                else:
                    state_dict_connections.add(source.get_outgoing_connection_to(target))

            # remove internal connections
            for connection in self.get_internal_connections():
                if connection not in state_dict_connections:
                    self.logger.info(f"Removing connection inside a clone between "
                                     f"`{connection.get_source().get_name()}` and "
                                     f"`{connection.get_target().get_name()}` as part of synchronization with the proxy.")
                    connection.delete()

            # internal cells
            internal_cells_state = state[self.__INTERNAL_CELLS_KEY]
            for cell in self.get_internal_cells():
                cell._set_sync_state(internal_cells_state[cell.get_name()])

    def supports_scaling(self) -> bool:
        support_scaling = True
        for cell in self.get_internal_cells():
            if not cell.supports_scaling():
                support_scaling = False
                break
        return support_scaling

    # ------ Topology ------

    def get_internal_topology(self) -> "Topology":
        if not self._internal_topology_is_set():
            self._set_internal_topology()
        assert self.__internal_topology is not None
        return self.__internal_topology

    def can_have_as_internal_topology(self, topology: Optional["Topology"]) -> BoolExplained:
        if topology is None:
            return TrueExplained()
        if not isinstance(topology, Topology):
            return FalseExplained(f"{self}: internal topology should be of type Topology, got {type(topology)}.")
        return TrueExplained()

    def _set_internal_topology(self) -> None:
        if self.is_deployed():
            raise AlreadyDeployedException(f"Cannot (re)set the internal topology of {self} when it is deployed.")
        self.__internal_topology = select_a_topology(self)
        for cell in self.get_internal_cells():
            if isinstance(cell, ICompositeCell):
                cell._set_internal_topology()

    def _clear_internal_topology(self) -> None:
        if self.is_deployed():
            raise AlreadyDeployedException(f"Cannot (re)set the internal topology of {self} when it is deployed.")
        if self.__internal_topology is not None:
            self.__internal_topology.delete()
        self.__internal_topology = None
        for cell in self.get_internal_cells():
            if isinstance(cell, ICompositeCell):
                cell._clear_internal_topology()

    def _internal_topology_is_set(self) -> bool:
        if self.__internal_topology is None:
            return False
        for cell in self.get_internal_cells():
            if isinstance(cell, ICompositeCell) and not cell._internal_topology_is_set():
                return False
        return True

    def assert_has_proper_internal_topology(self) -> None:
        if not self._internal_topology_is_set():
            return
        topology = self.get_internal_topology()
        raise_if_not(self.can_have_as_internal_topology(topology), InvalidStateException)
        if topology.get_parent_cell() != self:
            raise InvalidStateException(f"Inconsistent relation: {topology} doesn't have {self} as parent cell.")
        topology.assert_is_valid()

    def get_topology_description(self) -> Dict[str, Any]:
        description = super(ACompositeCell, self).get_topology_description()
        description["internal_cells_topologically_ordered"] = [cell.get_topology_description() for cell in self.get_internal_cells_in_topological_order()]
        description["internal_connections"] = [connection.get_topology_description() for connection in self.get_internal_connections()]
        return description

    def _has_as_internal_recurrent_connection(self, internal_connection: "IConnection") -> bool:
        topology = self.get_internal_topology()
        return topology.has_as_recurrent_connection(internal_connection)

    def _has_as_internal_source_cell(self, internal_cell: "ICell") -> bool:
        topology = self.get_internal_topology()
        return topology.has_as_source_cell(internal_cell)

    def _has_as_internal_sink_cell(self, internal_cell: "ICell") -> bool:
        topology = self.get_internal_topology()
        return topology.has_as_sink_cell(internal_cell)

    def get_internal_cells_in_topological_order(self) -> "Sequence[ICell]":
        topology = self.get_internal_topology()
        return tuple(topology.get_ordered_cells())

    # ------ Pulling ------

    def _on_pull(self) -> None:
        raise NotImplementedError

    def _on_reset(self) -> None:
        super(ACompositeCell, self)._on_reset()
        for internal_cell in self.get_internal_cells():
            internal_cell.reset()

    def get_nb_required_gpus(self) -> float:
        return sum([internal_cell.get_nb_required_gpus() for internal_cell in self.get_internal_cells()])

    # ------ General validation ------

    def assert_is_valid(self) -> None:
        super(ACompositeCell, self).assert_is_valid()
        self.assert_has_proper_internal_cells()
        self.assert_has_proper_internal_connections()
        self.assert_has_proper_internal_topology()

    # ------ Deletion ------

    def delete(self) -> None:
        raise_if_not(self.can_be_deleted(), CannotBeDeletedException)
        self._clear_internal_topology()
        for internal_connection in self.get_internal_connections():
            internal_connection.delete()
        for internal_cell in self.get_internal_cells():
            internal_cell.delete()
        super(ACompositeCell, self).delete()
