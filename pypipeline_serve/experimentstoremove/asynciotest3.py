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