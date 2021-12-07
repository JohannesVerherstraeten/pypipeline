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
        """
        Returns:
            All inputs of this cell, and those of its internal cells.
        """
        result: List["IInput"] = []
        result.extend(self.get_inputs())
        for cell in self.get_internal_cells():
            result.extend(cell.get_inputs_recursively())
        return result

    def get_input_recursively(self, full_name: str) -> "IInput":
        """
        Get the input with the given full name.

        If the input belongs to this cell, it should be just the ordinary input name.
        If the input belongs to an internal cell, the full name should be relative to this cell.

        Args:
            full_name: the full name of the input to get.
        Returns:
            The input of this cell, or one of the internal cells, with the given full name.
        Raises:
            KeyError: this cell and its internal cells have no input with the given name.
        """
        if "." not in full_name:
            return self.get_input(full_name)
        cell_name, io_name = full_name.split(".", maxsplit=1)
        cell = self.get_internal_cell_with_name(cell_name)      # TODO may raise exceptions
        return cell.get_input_recursively(io_name)

    def get_outputs_recursively(self) -> Sequence["IOutput"]:
        """
        Returns:
            All outputs of this cell, and those of its internal cells.
        """
        result: List["IOutput"] = []
        result.extend(self.get_outputs())
        for cell in self.get_internal_cells():
            result.extend(cell.get_outputs_recursively())
        return result

    def get_output_recursively(self, full_name: str) -> "IOutput":
        """
        Get the output with the given full name.

        If the output belongs to this cell, it should be just the ordinary output name.
        If the output belongs to an internal cell, the full name should be relative to this cell.

        Args:
            full_name: the full name of the output to get.
        Returns:
            The output of this cell, or one of the internal cells, with the given full name.
        Raises:
            KeyError: this cell and its internal cells have no output with the given name.
        """
        if "." not in full_name:
            return self.get_output(full_name)
        cell_name, io_name = full_name.split(".", maxsplit=1)
        cell = self.get_internal_cell_with_name(cell_name)       # TODO may raise exceptions
        return cell.get_output_recursively(io_name)

    def _add_io(self, io: "IO") -> None:
        """
        Auxiliary mutator in the IO-ICell relation, as owner of the IO.
        -> Should only be used by IO instances in their __init__.

        Args:
            io: the IO (input/output) to add to this cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
        if io.get_name() in self.get_internal_cell_names():
            raise InvalidInputException(f"{self} already has a cell with name {io.get_name()}")
        super(ACompositeCell, self)._add_io(io)

    def assert_has_proper_io(self) -> None:
        """
        Raises:
            InvalidStateException: if one of this cell's IO is invalid.
        """
        super(ACompositeCell, self).assert_has_proper_io()
        internal_cell_names = self.get_internal_cell_names()
        for io in self.get_all_io():
            if io.get_name() in internal_cell_names:
                raise InvalidStateException(f"{self} has an internal cell with the same name as an io: "
                                            f"{io.get_name()}")

    # ------ Internal cells (A composite cell can be an observer of its internal cells)------

    def has_as_observable(self, observable: "IObservable") -> bool:
        """
        Args:
            observable: an observable object.
        Returns:
            True if this observer is observing the given observable.
        """
        if not self._wants_to_observe_its_internal_cells():
            return False
        if not isinstance(observable, pypipeline.cell.ICell):
            return False
        return self.has_as_internal_cell(observable)

    def update(self, event: "Event") -> None:
        """
        Will be called by the observables of this observer (ex: internal cell).

        Args:
            event: the event to notify this observer about.

        # TODO may raise exceptions
        """
        # Called when the state of an internal cell changes.
        self.notify_observers(event)

    def get_internal_cells(self) -> "Sequence[ICell]":
        """
        Returns:
            The internal cells of this composite cell (non-recursively).
        """
        return tuple(self.__internal_cells.values())

    def get_internal_cells_recursively(self) -> Sequence["ICell"]:
        """
        Returns:
            The internal cells of this composite cell recursively.
        """
        result = []
        for cell in self.get_internal_cells():
            result.append(cell)
            if isinstance(cell, ICompositeCell):
                result.extend(cell.get_internal_cells_recursively())
        return result

    def get_internal_cell_names(self) -> Sequence[str]:
        """
        Returns:
            The names of the internal cells of this composite cell (non-recursively).
        """
        return tuple(self.__internal_cells.keys())

    def get_internal_cell_with_name(self, name: str) -> "ICell":
        """
        Args:
            name: the name of an internal cell.
        Returns:
            The internal cell with the given name.
        Raises:
            KeyError: if no internal cell with the given name exists.
        """
        return self.__internal_cells[name]

    def _add_internal_cell(self, cell: "ICell") -> None:
        """
        Should only be used by ICell instances in their __init__.

        Main mutator in the ICompositeCell-ICell relation, as parent of internal cells.
        Main mutator in the IObserver-IObservable relation as potential observer of internal cells.

        Args:
            cell: the internal cell to add to this composite cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
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
        """
        Should only be used by ICell instances in their delete().

        Main mutator in the ICompositeCell-ICell relation, as parent of internal cells.
        Main mutator in the IObserver-IObservable relation as potential observer of internal cells.

        Args:
            cell: the internal cell to remove from this composite cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
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
        """
        Args:
            cell: internal cell to validate.
        Returns:
            TrueExplained if the given cell is a valid internal cell for this composite cell. FalseExplained otherwise.
        """
        # Can I have the cell as internal cell and observable?
        if not isinstance(cell, pypipeline.cell.icell.ICell):
            return FalseExplained(f"{self}: internal cell should be of type ICell, got {type(cell)}. ")
        return TrueExplained()

    def get_nb_internal_cells(self) -> int:
        """
        Returns:
            The number of internal cells inside this composite cell (non-recursively).
        """
        return len(self.__internal_cells)

    def get_max_nb_internal_cells(self) -> int:
        """
        Returns:
            The maximum amount of internal cells that can be nested inside this composite cell.
        """
        return self.__max_nb_internal_cells

    def can_have_as_nb_internal_cells(self, number_of_internal_cells: int) -> BoolExplained:
        """
        Main validator in the ICompositeCell-ICell relation, as parent of internal cells.

        Args:
            number_of_internal_cells: the number to validate.
        Returns:
            TrueExplained if the given number of internal cells is allowed. FalseExplained otherwise.
        """
        max_nb = self.get_max_nb_internal_cells()
        if not 0 <= number_of_internal_cells <= max_nb:
            return FalseExplained(f"{self}: the number of internal cells should be in range 0..{max_nb} (inclusive)")
        return TrueExplained()

    def has_as_internal_cell(self, cell: "ICell") -> bool:
        """
        Args:
            cell: a cell object
        Returns:
            True if the given cell is an internal cell of this composite cell. False otherwise.
        """
        return cell in self.__internal_cells.values()

    def has_as_internal_cell_name(self, name: str) -> bool:
        """
        Args:
            name: a cell name
        Returns:
            True if this composite cell has an internal cell with the given name. False otherwise.
        """
        return name in self.__internal_cells

    def assert_has_proper_internal_cells(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the internal cells is invalid.
        """
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
        """
        Should only be used by IConnection instances in their __init__.

        Auxiliary mutator in the IConnection-ICompositeCell relation, as the parent cell of the connection.


        Args:
            connection: the internal connection to add.
        Raises:
            AlreadyDeployedException: if this cell is already deployed
            InvalidInputException
        """
        self._clear_internal_topology()
        raise_if_not(self.can_have_as_internal_connection(connection), InvalidInputException)
        if self.has_as_internal_connection(connection):
            raise InvalidInputException(f"{self} already has a connection {connection}")
        self.__internal_connections.append(connection)

    def _remove_internal_connection(self, connection: "IConnection") -> None:
        """
        Should only be used by IConnection instances in their delete().

        Auxiliary mutator in the IConnection-ICompositeCell relation, as the parent cell of the connection.

        Args:
            connection: the internal connection to remove.
        Raises:
            AlreadyDeployedException: if this cell is already deployed
            InvalidInputException
        """
        self._clear_internal_topology()
        if not self.has_as_internal_connection(connection):
            raise InvalidInputException(f"{self} doesn't have a connection {connection}")
        self.__internal_connections.remove(connection)

    def can_have_as_internal_connection(self, connection: "IConnection") -> BoolExplained:
        """
        Args:
            connection: connection to validate
        Returns:
            TrueExplained if this connection is a valid internal connection, FalseExplained otherwise.
        """
        if not isinstance(connection, pypipeline.connection.connection.IConnection):
            return FalseExplained(f"{self}: internal connection should be of type IConnection, got {type(connection)}.")
        return TrueExplained()

    def get_internal_connections(self) -> "Sequence[IConnection]":
        """
        Returns:
            The internal connections of this composite cell (non-recursively).
        """
        return tuple(self.__internal_connections)

    def has_as_internal_connection(self, connection: "IConnection") -> bool:
        """
        Args:
            connection: a connection object.
        Returns:
            True if this composite cell has the given connection as an internal connection, False otherwise.
        """
        return connection in self.__internal_connections

    def assert_has_proper_internal_connections(self) -> None:
        """
        Raises:
            InvalidStateException: if one or more of the internal connections are invalid.
        """
        for connection in self.get_internal_connections():
            raise_if_not(self.can_have_as_internal_connection(connection), InvalidStateException)
            if connection.get_parent_cell() != self:
                raise InvalidStateException(f"Inconsistent relation: {connection} doesn't have {self} as parent cell.")

            connection.assert_is_valid()

    # ------ Deployment ------

    def is_deployable(self) -> "BoolExplained":
        """
        Returns:
            TrueExplained if all preconditions for deployment are met. FalseExplained otherwise.
        """
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
        """
        Returns:
            TrueExplained if all inputs of this cell are provided (they have an incoming connection, a default,
            value, ...)
        """
        result: BoolExplained = super(ACompositeCell, self).inputs_are_provided()
        for cell in self.get_internal_cells():
            result *= cell.inputs_are_provided()
        return result

    def _on_deploy(self) -> None:
        """
        Override this method to add functionality that must happen when deploying the cell.

        Don't forget to call the _on_deploy of the super-class when overriding this method! Ex:
        ```
        def _on_deploy(self) -> None:
            super(MyCell, self)._on_deploy()
            # other deployment code
        ```

        Raises:
            AlreadyDeployedException: if called when an internal cell is already deployed.
            IndeterminableTopologyException: if the internal topology could not be determined.
                Cannot happen in deploy(), as the is_deployable() method covers this case already.
            Exception: any exception that the user may raise when overriding _on_deploy or _on_undeploy.
        """
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
        """
        Override this method to add functionality that must happen when undeploying the cell.

        Don't forget to call the _on_undeploy of the super-class when overriding this method! Ex:
        ```
        def _on_undeploy(self) -> None:
            super(MyCell, self)._on_undeploy()
            # other undeployment code
        ```

        Raises:
            NotDeployedException: if called when an internal cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_undeploy.
        """
        super(ACompositeCell, self)._on_undeploy()
        for cell in reversed(self.get_internal_cells_in_topological_order()):
            cell.undeploy()

    # ------ State synchronization between a cell and its clones ------

    def _get_sync_state(self) -> Dict[str, Any]:
        """
        Used for synchronizing the state of clone cells with that of their corresponding original one.

        Returns:
            A (nested) dictionary, containing the state of this cell.
        """
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
        """
        Used for synchronizing the state of clone cells with that of their corresponding original one.

        Args:
            state: a (nested) dictionary, containing the new state of this cell.
        Raises:
            KeyError: when the state dictionary contains the name of a cell or IO that is not available in this cell.
        """
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
        """
        A cell support scaling if it supports multiple clones of it running in parallel. This should usually be False
        for stateful cells.

        Returns:
            True if this cell supports scaling. False otherwise.
        """
        support_scaling = True
        for cell in self.get_internal_cells():
            if not cell.supports_scaling():
                support_scaling = False
                break
        return support_scaling

    # ------ Topology ------

    def get_internal_topology(self) -> "Topology":
        """
        Returns:
            The internal topology of this composite cell: which of the internal cells are sources and/or sinks,
            and which internal connections are recurrent or not?
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
        """
        if not self._internal_topology_is_set():
            self._set_internal_topology()
        assert self.__internal_topology is not None
        return self.__internal_topology

    def can_have_as_internal_topology(self, topology: Optional["Topology"]) -> BoolExplained:
        """
        Args:
            topology: the topology to validate.
        Returns:
            TrueExplained if the given topology is a valid topology for this composite cell.
        """
        if topology is None:
            return TrueExplained()
        if not isinstance(topology, Topology):
            return FalseExplained(f"{self}: internal topology should be of type Topology, got {type(topology)}.")
        return TrueExplained()

    def _set_internal_topology(self) -> None:
        """
        Sets the internal topology of this cell. See cell.get_internal_topology.

        Raises:
            AlreadyDeployedException: if called when the cell is already deployed.
            IndeterminableTopologyException: if the internal topology could not be determined.
        """
        if self.is_deployed():
            raise AlreadyDeployedException(f"Cannot (re)set the internal topology of {self} when it is deployed.")
        self.__internal_topology = select_a_topology(self)
        for cell in self.get_internal_cells():
            if isinstance(cell, ICompositeCell):
                cell._set_internal_topology()

    def _clear_internal_topology(self) -> None:
        """
        Clears the internal topology of this cell. See cell.get_internal_topology.

        Raises:
            AlreadyDeployedException: if called when the cell is already deployed.
        """
        if self.is_deployed():
            raise AlreadyDeployedException(f"Cannot (re)set the internal topology of {self} when it is deployed.")
        if self.__internal_topology is not None:
            self.__internal_topology.delete()
        self.__internal_topology = None
        for cell in self.get_internal_cells():
            if isinstance(cell, ICompositeCell):
                cell._clear_internal_topology()

    def _internal_topology_is_set(self) -> bool:
        """
        Returns:
            True if the internal topology is set, False otherwise.
        """
        if self.__internal_topology is None:
            return False
        for cell in self.get_internal_cells():
            if isinstance(cell, ICompositeCell) and not cell._internal_topology_is_set():
                return False
        return True

    def assert_has_proper_internal_topology(self) -> None:
        """
        Raises:
            InvalidStateException: if the internal topology is not valid.
        """
        if not self._internal_topology_is_set():
            return
        topology = self.get_internal_topology()
        raise_if_not(self.can_have_as_internal_topology(topology), InvalidStateException)
        if topology.get_parent_cell() != self:
            raise InvalidStateException(f"Inconsistent relation: {topology} doesn't have {self} as parent cell.")
        topology.assert_is_valid()

    def get_topology_description(self) -> Dict[str, Any]:
        """
        Returns:
            A dictionary (json format), fully describing the topological properties if this cell.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        description = super(ACompositeCell, self).get_topology_description()
        description["internal_cells_topologically_ordered"] = [cell.get_topology_description() for cell in self.get_internal_cells_in_topological_order()]
        description["internal_connections"] = [connection.get_topology_description() for connection in self.get_internal_connections()]
        return description

    def _has_as_internal_recurrent_connection(self, internal_connection: "IConnection") -> bool:
        """
        Should only be used by IConnection objects, see connection.is_recurrent()

        Args:
            internal_connection: a connection object.
        Returns:
            True if the given internal connection is recurrent in the current topology, False otherwise.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
            AssertionError: if the connection is not an internal connection of this composite cell.
        """
        topology = self.get_internal_topology()
        return topology.has_as_recurrent_connection(internal_connection)

    def _has_as_internal_source_cell(self, internal_cell: "ICell") -> bool:
        """
        Should only be used by ICell objects: see cell.is_source_cell()

        Args:
            internal_cell: a cell object.
        Returns:
            True if the given internal cell is a source cell in the current topology, False otherwise.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
            AssertionError: if the cell is not an internal cell of this composite cell.
        """
        topology = self.get_internal_topology()
        return topology.has_as_source_cell(internal_cell)

    def _has_as_internal_sink_cell(self, internal_cell: "ICell") -> bool:
        """
        Should only be used by ICell objects: see cell.is_sink_cell()

        Args:
            internal_cell: a cell object.
        Returns:
            True if the given internal cell is a sink cell in the current topology, False otherwise.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
            AssertionError: if the cell is not an internal cell of this composite cell.
        """
        topology = self.get_internal_topology()
        return topology.has_as_sink_cell(internal_cell)

    def get_internal_cells_in_topological_order(self) -> "Sequence[ICell]":
        """
        The topological ordering represents an ordering in which the cells can be executed.

        If cell_a has an outgoing connection to an input of cell_b, cell_a will occur before cell_b in the
        topological order.

        Returns:
            The internal cells of this composite cell in topological order.
        Raises:
            IndeterminableTopologyException: if the internal topology could not be determined.
        """
        topology = self.get_internal_topology()
        return tuple(topology.get_ordered_cells())

    # ------ Pulling ------

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
        raise NotImplementedError

    def _on_reset(self) -> None:
        """
        Override this method to add functionality that must happen when resetting the cell.

        During a reset, a cell must clear its internal state. This doesn't include cell configuration, but only
        cell state that was accumulated during consecutive pulls.
        Ex: reset the dataloader iterator of a dataloader cell.
        Ex: the currently accumulated batch in a BatchingCell.

        Don't forget to call the _on_reset of the super-class when overriding this method! Ex:
        ```
        def _on_reset(self) -> None:
            super(MyCell, self)._on_reset()
            # other resetting code
        ```

        Raises:
            Exception: any exception that the user may raise when overriding _on_reset.
        """
        super(ACompositeCell, self)._on_reset()
        for internal_cell in self.get_internal_cells():
            internal_cell.reset()

    def get_nb_required_gpus(self) -> float:
        """
        Override this method to indicate how much GPUs your cell needs.

        Note: very experimental feature - might not yet work at all.

        Returns:
            The number of GPUs this cell needs. Can be a fraction.
        """
        return sum([internal_cell.get_nb_required_gpus() for internal_cell in self.get_internal_cells()])

    # ------ General validation ------

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if one of the attributes or relations of this cell is invalid.
        """
        super(ACompositeCell, self).assert_is_valid()
        self.assert_has_proper_internal_cells()
        self.assert_has_proper_internal_connections()
        self.assert_has_proper_internal_topology()

    # ------ Deletion ------

    def delete(self) -> None:
        """
        Deletes this cell, and all its internals.

        Raises:
            CannotBeDeletedException
        """
        raise_if_not(self.can_be_deleted(), CannotBeDeletedException)
        self._clear_internal_topology()
        for internal_connection in self.get_internal_connections():
            internal_connection.delete()
        for internal_cell in self.get_internal_cells():
            internal_cell.delete()
        super(ACompositeCell, self).delete()
