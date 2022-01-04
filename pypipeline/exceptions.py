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


class UnsetRecurrentConnectionException(Exception):
    """
    Raised when a CellOutput is recurrently pulled and it has no output value from previous timestep set yet.

    If the recurrent connection is wanted, you can provide an initial_value to the CellOutput during it's creation.
    """
    pass


class UnconnectedException(Exception):
    """
    Raised when a CellInput is being pulled without having an incoming connection.
    """
    pass


class IndeterminableTopologyException(Exception):
    """
    Raised when the topology of the cells cannot be determined.
    """
    pass


class NotDeployableException(Exception):
    """
    Raised when a cell cannot be deployed.
    """
    pass


class NoInputProvidedException(Exception):
    """
    Raised when an AInput instance has not yet been provided with a value.
    """
    pass


class NoOutputProvidedException(Exception):
    """
    Raised when an AOutput instance has not yet been provided with a value.
    """
    pass


class NotDeployedException(Exception):
    """
    Raised when functionality of a cell is called that is only available when it is deployed.
    """
    pass


class AlreadyDeployedException(Exception):
    """
    Raised when functionality of a cell is called that is not available anymore when it is deployed.
    """
    pass


class CannotBeDeletedException(Exception):
    """
    Raised when an object cannot be deleted.
    """
    pass


class InvalidStateException(Exception):
    """
    Raised when the cell/pipeline is in an illegal state. (Ex: inconsistent mutual references)
    """
    pass


# ====== Can only be raised when a pipeline is deployed ======


class NonCriticalException(Exception):
    """
    Raised when an exception occurs during the pulling of a PyPipeline component, but the component can recover
    and is fully functional again for the next pull.

    Should be used as base class for more specific exceptions.
    """
    pass


class InvalidInputException(NonCriticalException):
    """
    Raised when an invalid value is provided to an IO element, or when a modification to the pipeline/cell
    would bring it to an invalid state.
    """
    pass


class TimeoutException(Exception):
    """
    Raised when a call times out.
    """
    pass


class InactiveException(Exception):
    """
    Raised when an inactive cell is being pulled. ScalableCells are inactive when all internal threads have
    paused.
    """
    pass


class ScalingNotSupportedException(Exception):
    """
    Raised if the scaling up/down of a specific scalable cell is not supported.
    """
    pass
