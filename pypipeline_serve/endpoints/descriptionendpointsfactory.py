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
