import statistics
import time
from functools import partial
import slonik_proto as slonik

queries = [
    'SELECT 1::int2',
    'SELECT 2::int4',
    'SELECT 3::int8',
    'SELECT 4.0::float8',
    'SELECT 5::TEXT',
    'SELECT 6::BPCHAR',
    'SELECT 7::VARCHAR',    
    "SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid",
    'SELECT \'{"id": 8, "data": "aaa"}\'::json',
    'SELECT \'{"id": 9, "data": "bbb"}\'::jsonb',
    'SELECT generate_series(1, 100)',
    'SELECT generate_series(1, 1000)',
    'SELECT generate_series(1, 10000)',
    'SELECT generate_series(1, 100000)',
    "SELECT typname, typnamespace, typowner, typlen, typbyval, typcategory, typispreferred, typisdefined, typdelim, typrelid, \
        typelem, typarray from pg_type where typtypmod = -1 and typisdefined = true"
]
iterations = 100
use_prepared_statements = False

conn = slonik.RustPgConnection('postgresql://postgres@localhost:5433/slonik_test')

# warm up the connection
conn.query('SELECT 1')

for query in queries:
    times = [None] * iterations

    run = partial(conn.query, query)

    # a prepared statement doesn't seem super useful for most of the queries in this benchmark, but still
    if use_prepared_statements:
        stmt = conn.prepare(query)
        run = stmt.query

    print(f'benchmarking "{query}" {iterations} times')
    for i in range(iterations):
        start = time.perf_counter()

        # launch the query via the function pointer
        run()

        # for row in rows:
        #     # print(row[0], type(row[0]))
        #     print(row)

        times[i] = time.perf_counter() - start

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

