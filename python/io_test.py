import asyncio
import time

from functools import partial

import slonik_proto as slonik

def on_rust_future_resolved(loop, fut, value):
    loop.call_soon_threadsafe(fut.set_result, value)

def on_rust_blocked_on_read(loop, fd):
    print(f'python - rust blocked on read for fd {fd}')
    loop.add_reader(fd, on_rust_read_unblocked, loop, fd)

def on_rust_read_unblocked(loop, fd):
    loop.remove_reader(fd)
    print(f'\npython - rust unblocked on read for fd {fd}')
    slonik.on_fd_read_ready(fd)

def on_rust_blocked_on_write(loop, fd):
    print(f'python - rust blocked on write for fd {fd}')
    loop.add_writer(fd, on_rust_write_unblocked, loop, fd)

def on_rust_write_unblocked(loop, fd):
    loop.remove_writer(fd)
    print(f'\npython - rust unblocked on write for fd {fd}')
    slonik.on_fd_write_ready(fd)

async def spawn_rust_io_task(rust_task):
    # prepare bridge data
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    # set up completion and IO bridge callbacks
    resolve_future_in_loop = partial(on_rust_future_resolved, loop, fut)
    io_read_registrar = partial(on_rust_blocked_on_read, loop)
    io_write_registrar = partial(on_rust_blocked_on_write, loop)

    # put it all together and spawn the rust task
    rust_task(resolve_future_in_loop, io_read_registrar, io_write_registrar)

    # the rust result will resolve the future up via the completion callback 
    return await fut

def async_io_in_rust(delay):
    io_task = partial(slonik.async_io_example, delay)
    return spawn_rust_io_task(io_task)

async def main():
    print('sending HTTP GET to google.com with 5s delay\n')
    res = await async_io_in_rust(5000)

    print('\nresponse received:')
    print(res)

    print(f'\nmultiple requests example:')
    start = time.perf_counter()
    timers = [async_io_in_rust(4000), async_io_in_rust(1000), async_io_in_rust(2000)]
    await asyncio.gather(*timers)
    elapsed = time.perf_counter() - start

    print(f'gathering {len(timers)} tasks took {elapsed:0.4f} seconds')


asyncio.run(main())