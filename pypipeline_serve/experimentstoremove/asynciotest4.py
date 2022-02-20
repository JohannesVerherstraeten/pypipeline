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

import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import time
import asyncio


class ModelIn(BaseModel):
    input: int


class ModelOut(BaseModel):
    output: int


def long_function(value: ModelIn) -> ModelOut:
    time.sleep(3)
    return ModelOut(output=value.input + 1)


async def long_function_async(value: ModelIn) -> ModelOut:
    loop = asyncio.get_running_loop()

    ## Options:
    # 1. Run in the default loop's executor:
    pool = None
    # # 2. Run in a custom thread pool:
    # with concurrent.futures.ThreadPoolExecutor() as pool:
    # # 3. Run in a custom process pool:
    # with concurrent.futures.ProcessPoolExecutor() as pool:

    result = await loop.run_in_executor(
        pool, long_function, value)

    return result



app = FastAPI()


@app.post("/pull")
async def execute(input_model: ModelIn):
    return await long_function_async(input_model)


uvicorn.run(app, host="0.0.0.0", port=8810)
