import asyncio
import time

from functools import partial

import slonik_proto as slonik

def on_rust_future_resolved(loop, fut, value):
    loop.call_soon_threadsafe(fut.set_result, value)

async def spawn_rust_task(rust_task):
    loop = asyncio.get_running_loop()

    fut = loop.create_future()
    resolve_future_in_loop = partial(on_rust_future_resolved, loop, fut)

    rust_task(resolve_future_in_loop)

    return await fut

def sleep_in_rust(delay):
    sleep_task = partial(slonik.sleep_example, delay)
    return spawn_rust_task(sleep_task)

def async_add_in_rust(a, b):
    add_task = partial(slonik.async_add_example, a, b)
    return spawn_rust_task(add_task)

async def main():
    print('yo, big up')
    await asyncio.sleep(1)

    print("c'est dadane")
    res = await sleep_in_rust(500)

    print(f"big up à tout le: '{res}'")

    print(f'\nasync add example:')
    res = await async_add_in_rust(555, 111)
    print(f"async_add_in_rust(555, 111): '{res}'")

    print(f'\nmultiple timers example:')
    start = time.perf_counter()
    timers = [sleep_in_rust(500), sleep_in_rust(1000), sleep_in_rust(2000)]
    await asyncio.gather(*timers)
    elapsed = time.perf_counter() - start

    print(f'gathering {len(timers)} tasks took {elapsed:0.4f} seconds')

asyncio.run(main())