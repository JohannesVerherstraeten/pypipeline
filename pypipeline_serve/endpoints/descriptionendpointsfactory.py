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

from typing import Type, Dict, Optional, TYPE_CHECKING
from fastapi import FastAPI
from pydantic import BaseModel, create_model

from pypipeline_serve.endpoints.aendpointfactory import AEndpointFactory
from pypipeline_serve.endpoints.utils import EncodingManager


if TYPE_CHECKING:
    from pypipeline_serve.fastapiserver import FastAPIServer


class DescriptionEndpointsFactory(AEndpointFactory):

    TAG = "Pipeline description"

    def create_endpoints(self,
                         app: FastAPI,
                         cell: "FastAPIServer",
                         encoding_manager: Optional[EncodingManager] = None) -> None:
        self._create_topology_endpoint(app, cell)

    def _create_topology_endpoint(self, app: FastAPI, cell: "FastAPIServer") -> None:
        TopologyModelOut: Type[BaseModel] = create_model("TopologyModelOut",
                                                         topology=(Dict, cell.get_topology_description()))

        @app.get("/topology", response_model=TopologyModelOut, tags=[self.TAG],
                 description=f"Get a description of the topology of `{cell.get_name()}`.")
        def get_topology_description() -> Dict:
            return cell.get_topology_description()
