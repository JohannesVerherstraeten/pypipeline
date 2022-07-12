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

from typing import TYPE_CHECKING, Type
from time import time

from pypipeline.cell.compositecell.scalablecell.strategy.ascalingstrategy import AScalingStrategy
from pypipeline.exceptions import ScalingNotSupportedException

if TYPE_CHECKING:
    from pypipeline.cell.compositecell.scalablecell.scalablecelldeployment import ScalableCellDeployment
    from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell.iclonecell import ICloneCell


class NoScalingStrategy(AScalingStrategy):
    """
    No-Scaling strategy class.

    A scaling strategy determines how a scalable cell should be executed. ATM 2 strategies are implemented:
     - the CloningStrategy (default): allows to scale up using clones of the internal cell
     - the NoScalingStrategy: doesn't allow scaling and executes the scalable cell's internal cell in the mainthread,
       just like a Pipeline. Useful for debugging.
    """

    def __init__(self, scalable_cell_deployment: "ScalableCellDeployment"):
        """
        Args:
            scalable_cell_deployment: the scalable cell deployment where this scaling strategy belongs to.
        Raises:
            NotDeployableException – if the internal cell cannot be deployed.
            AlreadyDeployedException – if the internal cell is already deployed.
            NotDeployableException – if the internal cell cannot be deployed.
            Exception – any exception that the user may raise when overriding _on_deploy or _on_undeploy
        """
        super(NoScalingStrategy, self).__init__(scalable_cell_deployment)
        self.get_internal_cell().deploy()

    @classmethod
    def create(cls, scalable_cell_deployment: "ScalableCellDeployment") -> "NoScalingStrategy":
        return NoScalingStrategy(scalable_cell_deployment)

    def delete(self) -> None:
        self.get_internal_cell().undeploy()
        super(NoScalingStrategy, self).delete()

    def _on_pull(self) -> None:
        self.logger.debug(f"{self}.pull()")
        # Just execute the internal cell in the main thread.
        output_queue = self.get_scalable_cell_deployment().get_output_queue()
        queue_idx = output_queue.acquire_queue_index()

        internal_cell = self.get_internal_cell()
        t0 = time()
        internal_cell.pull()
        t1 = time()
        self.get_scalable_cell_deployment().get_scalable_cell()._get_pull_duration_metric().labels(clone="None").observe(t1 - t0)
        # assumes scalable cells have no other outputs than output_ports:
        for output in self.get_scalable_cell_deployment().get_scalable_cell().get_output_ports():
            output_incoming_connections = output.get_incoming_connections()
            assert len(output_incoming_connections) == 1
            value = output_incoming_connections[0].pull()
            output_queue.set(output, queue_idx, value)

        output_queue.signal_queue_index_ready(queue_idx)

    def reset(self) -> None:
        self.get_internal_cell().reset()

    def add_clone(self, method: Type["ICloneCell"]) -> None:
        raise ScalingNotSupportedException(f"Scalable cell strategy 'NoScalingStrategy' doesn't allow scaling up.")

    def remove_clone(self, method: Type["ICloneCell"]) -> None:
        raise ScalingNotSupportedException(f"Scalable cell strategy 'NoScalingStrategy' doesn't allow scaling down.")
