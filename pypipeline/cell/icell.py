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

from typing import Optional, Any, TYPE_CHECKING, Dict, Sequence

from pypipeline.cell.icellobserver import IObservable

if TYPE_CHECKING:
    from threading import RLock

    from pypipeline.cellio import IO, IInput, IOutput
    from pypipeline.cell.compositecell.icompositecell import ICompositeCell
    from pypipeline.cell.icellobserver import IObserver, Event
    from pypipeline.validation import BoolExplained


class ICell(IObservable):
    """
    Cell interface.

    An ICell is owned by its (optional) parent cell.
    An ICell owns its IO.

    An ICell is the controlled class in the ICompositeCell-ICell relation, as internal cell of a parent composite class.
    An ICell is the controlled class in the IObserver-IObservable relation, as observable.

    An ICell is the controlled class in the IO-ICell relation, as owner of the IO.
    """

    def _get_pull_lock(self) -> "RLock":
        raise NotImplementedError

    def get_name(self) -> str:
        """
        Returns:
            The name of this cell.
        """
        raise NotImplementedError

    def get_full_name(self) -> str:
        """
        Returns:
            The name of this cell, preceded by the full name of the parent cell.
            Format: grand_parent_name.parent_name.cell_name
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_name(cls, name: str) -> "BoolExplained":
        """
        Args:
            name: the name to validate.
        Returns:
            TrueExplained if the name is a valid name for this cell.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_name(self) -> None:
        """
        Raises:
            InvalidStateException: if the name of this cell is invalid.
        """
        raise NotImplementedError

    def get_parent_cell(self) -> "Optional[ICompositeCell]":
        """
        Returns:
            The parent cell of this cell, if it has one.
        """
        raise NotImplementedError

    def _set_parent_cell(self, parent_cell: "Optional[ICompositeCell]") -> None:
        """
        Auxiliary mutator in the ICompositeCell-ICell, as internal cell of a parent composite cell.
        -> Should only be used by ICompositeCell instances when registering this as internal cell.

        Args:
            parent_cell: the new parent cell.
        Raises:
            InvalidInputException
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_parent_cell(cls, cell: "Optional[ICompositeCell]") -> "BoolExplained":
        """
        Args:
            cell: the cell to validate.
        Returns:
            TrueExplained if the cell is a valid parent cell for this cell.
            FalseExplained otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_parent_cell(self) -> None:
        """
        Raises:
            InvalidStateException: if this cell doesn't have a valid parent cell.
        """
        raise NotImplementedError

    def get_all_io(self) -> Sequence["IO"]:
        """
        Returns:
            All inputs and outputs (IO) of this cell.
        """
        raise NotImplementedError

    def get_io(self, name: str) -> "IO":
        """
        Args:
            name: the name of the cell's input/output to get.
        Returns:
            This cell's input/output with the given name.
        Raises:
            KeyError: this cell has no IO with the given name.
        """
        raise NotImplementedError

    def get_io_names(self) -> Sequence[str]:
        """
        Returns:
            The names of all IO of this cell.
        """
        raise NotImplementedError

    def can_have_as_io(self, io: "IO") -> "BoolExplained":
        """
        Args:
            io: IO object (input or output) to validate.
        Returns:
            TrueExplained if the given IO is a valid IO for this cell. FalseExplained otherwise.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def _remove_io(self, io: "IO") -> None:
        """
        Auxiliary mutator in the IO-ICell relation, as owner of the IO.
        -> Should only be used by IO instances in their delete().

        Args:
            io: the IO (input/output) to remove from this cell.
        Raises:
            AlreadyDeployedException: if the cell is already deployed.
            InvalidInputException
        """
        raise NotImplementedError

    def assert_has_proper_io(self) -> None:
        """
        Raises:
            InvalidStateException: if one of this cell's IO is invalid.
        """
        raise NotImplementedError

    def has_as_io(self, io: "IO") -> bool:
        """
        Args:
            io: an input/output (IO) object.
        Returns:
            True if the given IO is part of this cell, False otherwise.
        """
        raise NotImplementedError

    def get_inputs(self) -> Sequence["IInput"]:
        """
        Returns:
            All inputs of this cell.
        """
        raise NotImplementedError

    def get_input_names(self) -> Sequence[str]:
        """
        Returns:
            The names of all inputs of this cell.
        """
        raise NotImplementedError

    def get_input(self, name: str) -> "IInput":
        """
        Raises:
            KeyError: this cell has no input with the given name.
        """
        raise NotImplementedError

    def get_inputs_recursively(self) -> Sequence["IInput"]:
        """
        Returns:
            All inputs of this cell, and those of its internal cells.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def has_as_input(self, cell_input: "IInput") -> bool:
        """
        Args:
            cell_input: an input object.
        Returns:
            True if the given input is part of this cell, false otherwise.
        """
        raise NotImplementedError

    def get_outputs(self) -> Sequence["IOutput"]:
        """
        Returns:
            All outputs of this cell.
        """
        raise NotImplementedError

    def get_output_names(self) -> Sequence[str]:
        """
        Returns:
            The names of all outputs of this cell.
        """
        raise NotImplementedError

    def get_output(self, name: str) -> "IOutput":
        """
        Raises:
            KeyError: this cell has no output with the given name.
        """
        raise NotImplementedError

    def get_outputs_recursively(self) -> Sequence["IOutput"]:
        """
        Returns:
            All outputs of this cell, and those of its internal cells.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def has_as_output(self, cell_output: "IOutput") -> bool:
        """
        Args:
            cell_output: an output object.
        Returns:
            True if the given output is part of this cell, false otherwise.
        """
        raise NotImplementedError

    def is_source_cell(self) -> bool:
        """
        Source cells are cells that provide data to a pipeline.

        Source cells in a pipeline are the ones that need to be executed before any of the other cells.
        See also sink cells. A cell can be both a source and a sink at the same time.

        Returns:
            True if this cell is a source cell.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        raise NotImplementedError

    def is_sink_cell(self) -> bool:
        """
        Sink cells are cells that remove data from a pipeline.

        Sink cells in a pipeline are the ones that can only be executed after all other cells.
        See also source cells. A cell can be both a source and a sink at the same time.

        Returns:
            True if this cell is a sink cell.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        raise NotImplementedError

    def get_topology_description(self) -> Dict[str, Any]:
        """
        Returns:
            A dictionary (json format), fully describing the topological properties if this cell.
        Raises:
            IndeterminableTopologyException: if the topology of this cell w.r.t. the surrounding cells could not be
                determined.
        """
        raise NotImplementedError

    def get_observers(self) -> Sequence["IObserver"]:
        """
        Returns:
            All observers of this cell.
        """
        raise NotImplementedError

    def _add_observer(self, observer: "IObserver") -> None:
        """
        Auxiliary mutator in the IObserver-IObservable relation, as observable.
        -> Should only be used by IObserver instances when registering this as observable.

        Args:
            observer: the observer to add.
        Raises:
            InvalidInputException
        """
        raise NotImplementedError

    def _remove_observer(self, observer: "IObserver") -> None:
        """
        Auxiliary mutator in the IObserver-IObservable relation, as observable.
        -> Should only be used by IObserver instances when unregistering this as observable.

        Args:
            observer: the observer to remove.
        Raises:
            InvalidInputException
        """
        raise NotImplementedError

    @classmethod
    def can_have_as_observer(cls, observer: "IObserver") -> "BoolExplained":
        """
        Auxiliary validator in the IObserver-IObservable relation, as observable.

        Args:
            observer: observer to validate
        Returns:
            TrueExplained if the given observer is valid. FalseExplained otherwise.
        """
        raise NotImplementedError

    def has_as_observer(self, observer: "IObserver") -> bool:
        """
        Args:
            observer: an observer object.
        Returns:
            True if the given observer is observing this cell, false otherwise.
        """
        raise NotImplementedError

    def assert_has_proper_observers(self) -> None:
        """
        Auxiliary validator in the IObserver-IObservable relation, as observable.

        Raises:
            InvalidStateException: if one of this cell's observers is invalid.
        """
        raise NotImplementedError

    def notify_observers(self, event: "Event") -> None:
        """
        Notify all observers of this cell that the given event just happened.

        Args:
            event: an event object, indicating what the observers should be notified of.
        Raises:
            TODO may raise exceptions?
        """
        raise NotImplementedError

    def _get_sync_state(self) -> Dict[str, Any]:
        """
        Used for synchronizing the state of clone cells with that of their corresponding original one.

        Returns:
            A (nested) dictionary, containing the state of this cell.
        """
        raise NotImplementedError

    def _set_sync_state(self, state: Dict) -> None:
        """
        Used for synchronizing the state of clone cells with that of their corresponding original one.

        Args:
            state: a (nested) dictionary, containing the new state of this cell.
        Raises:
            KeyError: when the state dictionary contains the name of a cell or IO that is not available in this cell.
        """
        raise NotImplementedError

    def clone(self, new_parent: "Optional[ICompositeCell]") -> "ICell":
        """
        Clone this cell to a new parent cell.

        Override this method if your cell instantiation requires more/other arguments than implemented here.
        If the cell doesn't support being cloned, raise NotImplementedError.

        Args:
            new_parent: the parent to which this cell should be cloned.
        Returns:
            The cloned cell.
        Raises:
            NotImplementedError: if this cell doesn't support cloning.
        """
        raise NotImplementedError

    def supports_scaling(self) -> bool:
        """
        A cell support scaling if it supports multiple clones of it running in parallel. This should usually be False
        for stateful cells.

        Returns:
            True if this cell supports scaling. False otherwise.
        """
        raise NotImplementedError

    def inputs_are_provided(self) -> "BoolExplained":
        """
        Returns:
            TrueExplained if all inputs of this cell are provided (they have an incoming connection, a default,
            value, ...)
        """
        raise NotImplementedError

    def is_deployable(self) -> "BoolExplained":
        """
        Returns:
            TrueExplained if all preconditions for deployment are met. FalseExplained otherwise.
        """
        raise NotImplementedError

    def deploy(self) -> None:
        """
        Deploy this cell.

        Deploying prepares a cell to be executed. This includes (not exhaustively):
         - validating whether all preconditions for deployment are met
         - acquiring resources needed to run the cell
         - spawning cell clones

        Raises:
            AlreadyDeployedException: if this cell is already deployed.
            NotDeployableException: if the cell cannot be deployed because some preconditions are not met.
            Exception: any exception that the user may raise when overriding _on_deploy or _on_undeploy.
        """
        raise NotImplementedError

    def undeploy(self) -> None:
        """
        Undeploy this cell.

        Opposite of deploying. This includes (not exhaustively):
         - releasing resources that the cell acquired
         - removing all spawned cell clones

        Raises:
            NotDeployedException: if this cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_undeploy.
        """
        raise NotImplementedError

    def is_deployed(self) -> bool:
        """
        Returns:
            True if this cell is deployed, False otherwise.
        """
        raise NotImplementedError

    def assert_is_properly_deployed(self) -> None:
        """
        Raises:
            InvalidStateException: if the cell is not properly deployed.
        """
        raise NotImplementedError

    def pull_as_output(self, output: "IOutput") -> None:
        """
        Cells should only execute again when all their outputs have pulled the previous result.
        This is to avoid that the cell is executed again while another downstream cell still needed an old output
        value.
        For example: imagine two ScalableCells each connected to a different output of a source cell. If the first
        scalablecell executes faster than the second one, it might request to pull the source cell multiple times while
        the other has only pulled the source cell once. If this pull would not block, the other scalablecell would miss
        some source outputs.

        Note: calling this method may block forever when an output pulls twice before all other outputs have
        pulled.

        Args:
            output: the output that wants to pull this cell.
        Raises:
            NotDeployedException: if the cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_pull.
            TODO may raise exceptions (more than written here, same as self.pull() ?)
        """
        raise NotImplementedError

    def pull(self) -> None:
        """
        During a pull, a cell will pull its inputs, execute it's functionality and set its outputs.

        Since a cell pulls its inputs before it executes, all prerequisite cells are pulled recursively up to
        the source cells, before this pull finishes.

        Raises:
            NotDeployedException: if the cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_pull.
            # TODO may raise exceptions (more exceptions in case of scalable cells etc...)

        Won't raise:
            IndeterminableTopologyException: this method can only be called when the cell is already deployed.
        """
        raise NotImplementedError

    def all_outputs_have_been_pulled(self) -> bool:
        """
        Returns:
            True if all the outputs of this cell have been pulled, meaning that they are ready to receive new
            output values (a new cell.pull() can be done).
        """
        raise NotImplementedError

    def _notify_connection_has_pulled(self) -> None:
        """
        Notifies the cell that one of the outgoing connections successfully pulled, and that other threads waiting
        for the pull lock may be notified.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def reset(self) -> None:
        """
        Reset the internal state of a cell and its prerequisite cells.

        Just like the pull() method, this reset is recursively propagated through all prerequisite cells
        up to the source cells.

        Resetting internal state does not include cell configuration, but only cell state that was accumulated
        during consecutive pulls.
        Ex: reset the dataloader iterator of a dataloader cell.
        Ex: the currently accumulated batch in a BatchingCell.

        Raises:
            NotDeployedException: if the cell is not deployed.
            Exception: any exception that the user may raise when overriding _on_reset.
            # TODO may raise exceptions (more exceptions in case of scalable cells etc...)
        """
        raise NotImplementedError

    def get_nb_required_gpus(self) -> float:
        """
        Override this method to indicate how much GPUs your cell needs.

        Note: very experimental feature - might not yet work at all.

        Returns:
            The number of GPUs this cell needs. Can be a fraction.
        """
        raise NotImplementedError

    def assert_is_valid(self) -> None:
        """
        Raises:
            InvalidStateException: if one of the attributes or relations of this cell is invalid.
        """
        raise NotImplementedError

    def delete(self) -> None:       # TODO all cells should override this correctly
        """
        Deletes this cell, and all its internals.

        Raises:
            AlreadyDeployedException: if the cell is deployed.
            InvalidInputException
        """
        raise NotImplementedError
