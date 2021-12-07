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
