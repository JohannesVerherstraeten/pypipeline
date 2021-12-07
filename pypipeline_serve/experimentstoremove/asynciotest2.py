import asyncio
import concurrent.futures
import time
import logging

logging.basicConfig(level=logging.INFO, format="[%(threadName)10s] [%(name)10s] [%(levelname)s] %(message)s")
logger = logging.getLogger()

def blocking_io():
    # File operations (such as logging) can block the
    # event loop: run them in a thread pool.
    logger.info(f"flag1")
    time.sleep(1)
    with open('/dev/urandom', 'rb') as f:
        return f.read(100)

def cpu_bound():
    # CPU-bound operations will block the event loop:
    # in general it is preferable to run them in a
    # process pool.
    logger.info(f"flag2")
    time.sleep(1)
    return sum(i * i for i in range(10 ** 7))

async def main():
    loop = asyncio.get_running_loop()

    ## Options:

    # 1. Run in the default loop's executor:
    result = await loop.run_in_executor(
        None, blocking_io)
    print('default thread pool', result)

    # 2. Run in a custom thread pool:
    with concurrent.futures.ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(
            pool, blocking_io)
        print('custom thread pool', result)

    # 3. Run in a custom process pool:
    with concurrent.futures.ProcessPoolExecutor() as pool:
        result = await loop.run_in_executor(
            pool, cpu_bound)
        print('custom process pool', result)


async def real_main():
    await asyncio.gather(main(), main(), main())

asyncio.run(real_main())
