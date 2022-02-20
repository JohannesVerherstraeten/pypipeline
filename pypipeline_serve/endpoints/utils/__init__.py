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

from pypipeline_serve.endpoints.utils.encoding import EncodingManager, Encoder, Decoder
from pypipeline_serve.endpoints.utils.pydanticmodels import get_generic_type, create_pydantic_model, decorate_as_form, \
    set_values, get_values, create_pydantic_model_recursively
