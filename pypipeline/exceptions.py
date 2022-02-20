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
