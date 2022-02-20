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

import asyncio
import concurrent.futures
import time
import logging

logging.basicConfig(level=logging.INFO, format="[%(threadName)10s] [%(name)10s] [%(levelname)s] %(message)s")
logger = logging.getLogger()


def run_pipeline(input_int: int) -> int:
    logger.info(f"Running pipeline with input {input_int}")
    time.sleep(5)
    return input_int + 1


async def run_pipeline_async(input_int: int) -> int:
    loop = asyncio.get_running_loop()

    ## Options:
    # 1. Run in the default loop's executor:
    pool = None
    # # 2. Run in a custom thread pool:
    # with concurrent.futures.ThreadPoolExecutor() as pool:
    # # 3. Run in a custom process pool:
    # with concurrent.futures.ProcessPoolExecutor() as pool:

    result = await loop.run_in_executor(
        pool, run_pipeline, input_int)

    return result


async def execution_call(sleep: int) -> int:
    await asyncio.sleep(sleep)
    result = await run_pipeline_async(sleep)
    logger.info(f"Result: {result}")
    return result


async def real_main():
    result = await asyncio.gather(execution_call(0), execution_call(2), execution_call(3))
    logger.info(f"Main result: {result}")
    return result


t0 = time.time()
final_result = asyncio.run(real_main())
t1 = time.time()
logger.info(f"Final result: {final_result}")
logger.info(f"Total time: {t1-t0}")