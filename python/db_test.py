import asyncio
import statistics
import time

from functools import partial

import slonik_proto as slonik


def on_rust_future_resolved(loop, fut, value):
    loop.call_soon_threadsafe(fut.set_result, value)

def on_rust_blocked_on_read(loop, fd):
    # print(f'python - rust blocked on read for fd {fd}')
    loop.add_reader(fd, on_rust_read_unblocked, loop, fd)

def on_rust_read_unblocked(loop, fd):
    loop.remove_reader(fd)
    # print(f'\npython - rust unblocked on read for fd {fd}')
    slonik.on_fd_read_ready(fd)

def on_rust_blocked_on_write(loop, fd):
    # print(f'python - rust blocked on write for fd {fd}')
    loop.add_writer(fd, on_rust_write_unblocked, loop, fd)

def on_rust_write_unblocked(loop, fd):
    loop.remove_writer(fd)
    # print(f'\npython - rust unblocked on write for fd {fd}')
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

def async_query_in_rust(query):
    db_task = partial(slonik.async_sqlx_example, query)
    return spawn_rust_io_task(db_task)

queries = [
    ('SELECT 1::int2', ['int2']),
    ('SELECT 2::int4', ['int4']),
    ('SELECT 3::int8', ['int8']),
    ('SELECT 4.0::float4', ['float4']),
    ('SELECT 4.5::float8', ['float8']),
    ('SELECT 5::TEXT', ['TEXT']),
    ('SELECT 6::BPCHAR', ['BPCHAR']),
    ('SELECT 7::VARCHAR', ['VARCHAR']),
    ("SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid", ['uuid']),
    ('SELECT \'{"id": 8, "data": "aaa"}\'::json', ['json']),
    ('SELECT \'{"id": 9, "data": "bbb"}\'::jsonb', ['jsonb']),
    ('SELECT true, false', ['bool', 'bool']),
    (
        'SELECT 1::int2, \
        2::int4, \
        3::int8, \
        4.0::float8, \
        5::TEXT, \
        6::BPCHAR, \
        7::VARCHAR, \
        \'3d9d291d-8668-480f-98bf-46ee10d07a5d\'::uuid, \'{"id": 8, "data": "aaa"}\'::json, \
        \'{"id": 9, "data": "bbb"}\'::jsonb',
        [
            'int2',
            'int4',
            'int8',
            'float8',
            'TEXT',
            'BPCHAR',
            'VARCHAR',
            'uuid',
            'json',
            'jsonb',
        ],
    ),
    ('SELECT generate_series(1, 10)', ['int4']),
    ('SELECT generate_series(1, 100)', ['int4']),
    ('SELECT generate_series(1, 1000)', ['int4']),
    ('SELECT generate_series(1, 10000)', ['int4']),
    ('SELECT generate_series(1, 100000)', ['int4']),
    ("SELECT typname, typnamespace, typowner, typlen, typbyval, typcategory, typispreferred, typisdefined, typdelim, typrelid, \
        typelem, typarray from pg_type where typtypmod = -1 and typisdefined = true", 
        ['name', 'oid', 'oid', 'int2', 'bool', 'text', 'bool', 'bool', 'text', 'oid', 'oid', 'oid']
    )
]
iterations = 100

conn = slonik.SqlxConnection('postgresql://postgres@localhost:5433/slonik_test')

async def main():
    print('executing async pg query')

    value = 29
    res = await async_query_in_rust(f'SELECT {value}, pg_sleep(1)')
    assert res == value

    print(f"big up à tout le: '{res}'")

    # warm up the connection
    await spawn_rust_io_task(partial(conn.query, 'SELECT 1', ['int2']))

    print()
    for (query, columns) in queries:
        times = [None] * iterations

        db_task = partial(conn.query, query, columns)

        print(f'benchmarking "{query}" {iterations} times')
        for i in range(iterations):
            start = time.perf_counter()
            rows = await spawn_rust_io_task(db_task)
            times[i] = time.perf_counter() - start

            # for row in rows:
            #     print(row)
            #     # print(row[0], type(row[0]))

        median = statistics.median(times)
        mean = statistics.mean(times)
        max_ = max(times)
        min_ = min(times)
        print(
            f"Mean: {mean:.4f} - "
            f"Median: {median:.4f} - "
            f"Max: {max_:.4f} - "
            f"Min: {min_:.4f}"
        )
        print('---')


asyncio.run(main())
