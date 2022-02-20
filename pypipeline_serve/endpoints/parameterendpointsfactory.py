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

from typing import Optional, TypeVar, TYPE_CHECKING
from fastapi import FastAPI

from pypipeline.cellio import RuntimeParameter
from pypipeline_serve.endpoints.aendpointfactory import AEndpointFactory
from pypipeline_serve.endpoints.utils import EncodingManager
from pypipeline_serve.endpoints.utils.pydanticmodels import create_pydantic_model_recursively, set_values

if TYPE_CHECKING:
    from pypipeline_serve.fastapiserver import FastAPIServer


EncodedType = TypeVar("EncodedType")
IOType = TypeVar("IOType")


class ParameterEndpointsFactory(AEndpointFactory):

    TAG = "Runtime parameters"

    def __init__(self):
        super(ParameterEndpointsFactory, self).__init__()

    def create_endpoints(self,
                         app: FastAPI,
                         cell: "FastAPIServer",
                         encoding_manager: Optional[EncodingManager] = None) -> None:
        self._create_set_runtime_params_endpoint(app, cell, encoding_manager)

    def _create_set_runtime_params_endpoint(self,
                                            app: FastAPI,
                                            cell: "FastAPIServer",
                                            encoding_manager: Optional[EncodingManager] = None) -> None:
        internal_cell = cell._get_internal_cell()       # access to protected member on purpose
        runtime_params = [input_ for input_ in internal_cell.get_inputs_recursively() if isinstance(input_,
                                                                                                    RuntimeParameter)]
        # RuntimeParamModelIn = create_pydantic_model("RuntimeParamModelIn", runtime_params, encoding_manager)
        RuntimeParamModelIn = create_pydantic_model_recursively("RuntimeParamModelIn", cell, runtime_params,
                                                                encoding_manager, full_model_required=False)

        @app.post("/parameters", tags=[self.TAG],
                  description=f"Update the runtime parameters of `{cell.get_name()}`.")
        def set_runtime_parameters(config_values: RuntimeParamModelIn) -> None:
            internal_cell = cell._get_internal_cell()
            internal_cell_config = getattr(config_values, internal_cell.get_name())
            set_values(internal_cell, internal_cell_config, encoding_manager)
