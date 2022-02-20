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

from typing import Optional, TYPE_CHECKING, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pypipeline.cellio import ConfigParameter, RuntimeParameter
from pypipeline_serve.endpoints.aendpointfactory import AEndpointFactory
from pypipeline_serve.endpoints.utils import EncodingManager
from pypipeline_serve.endpoints.utils.pydanticmodels import create_pydantic_model_recursively, set_values

if TYPE_CHECKING:
    from pypipeline.cell import ICell
    from pypipeline_serve.fastapiserver import FastAPIServer


class DeploymentEndpointsFactory(AEndpointFactory):

    TAG = "Deployment and configuration"

    def __init__(self):
        super(DeploymentEndpointsFactory, self).__init__()

    def create_endpoints(self,
                         app: FastAPI,
                         cell: "FastAPIServer",
                         encoding_manager: Optional[EncodingManager] = None) -> None:
        self._create_deployment_status_endpoint(app, cell)
        self._create_deploy_endpoint(app, cell, encoding_manager)
        self._create_undeploy_endpoint(app, cell)

    def _create_deployment_status_endpoint(self, app: FastAPI, cell: "FastAPIServer") -> None:
        @app.get("/deployment", tags=[self.TAG],
                 description=f"Query the deployment status of `{cell.get_name()}`.",
                 response_model=IsDeployedOut)
        def get_deployment_status() -> IsDeployedOut:
            return IsDeployedOut(pipeline_name=cell.get_full_name(),
                                 is_deployed=cell.is_deployed())

    def _create_deploy_endpoint(self,
                                app: FastAPI,
                                cell: "FastAPIServer",
                                encoding_manager: Optional[EncodingManager] = None) -> None:
        internal_cell = cell._get_internal_cell()       # access to protected member on purpose
        config_params = [input_ for input_ in internal_cell.get_inputs_recursively()
                         if isinstance(input_, ConfigParameter)]
        ConfigModelIn = create_pydantic_model_recursively("ConfigModelIn", cell, config_params, encoding_manager,
                                                          full_model_required=False)

        @app.post("/deployment", tags=[self.TAG],
                  description=f"(Re)deploy `{cell.get_name()}` with the given configuration parameters.",
                  response_model=IsDeployedOut)
        def configure_and_deploy(config_values: ConfigModelIn) -> IsDeployedOut:
            if cell.is_deployed():
                cell.undeploy()     # TODO may raise exceptions
            internal_cell_config = getattr(config_values, internal_cell.get_name())     # TODO getattr is not clean
            set_values(internal_cell, internal_cell_config, encoding_manager)
            self.__raise_if_no_parameter_defaults_set(cell)
            cell.deploy()       # TODO may raise exceptions
            return IsDeployedOut(pipeline_name=cell.get_full_name(),
                                 is_deployed=cell.is_deployed())

    def __raise_if_no_parameter_defaults_set(self, cell: "ICell") -> None:
        params_without_default: List[str] = []
        for input_ in cell.get_inputs_recursively():
            if isinstance(input_, ConfigParameter):      # TODO this stinks
                if not input_.value_is_set():
                    params_without_default.append(str(input_))
            elif isinstance(input_, RuntimeParameter):
                if not input_.default_value_is_set():       # TODO what if provided by an incoming connection?
                    params_without_default.append(str(input_))
        if len(params_without_default) > 0:
            params_pretty = f"- " + "\n- ".join(params_without_default)
            error_msg = f"Please provide default values for the following parameters before deploying: \n" \
                        f"{params_pretty}\n" \
                        f"Option 1 - via the API: \n" \
                        f"For configuration parameters: provide values in the body of the `POST /deployment` request. \n" \
                        f"For runtime parameters: provide values in the body of the `POST /parameters` request. \n" \
                        f"Option 2 - in the code: \n" \
                        f"For every parameter p, provide a default value with `p.set_value(<value>)`"
            self.logger.info(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)

    def _create_undeploy_endpoint(self, app: FastAPI, cell: "FastAPIServer") -> None:
        @app.delete("/deployment", tags=[self.TAG],
                    description=f"Undeploy `{cell.get_name()}` to release it's acquired resources.",
                    response_model=IsDeployedOut)
        def undeploy() -> IsDeployedOut:
            if cell.is_deployed():
                cell.undeploy()     # TODO may raise exceptions
            return IsDeployedOut(pipeline_name=cell.get_full_name(),
                                 is_deployed=cell.is_deployed())


class IsDeployedOut(BaseModel):
    pipeline_name: str = Field(description="The name of the pipeline that is linked to this API. ",
                               example="example_pipeline")
    is_deployed: bool = Field(description="Whether the pipeline is deployed or not. ", example=False)
