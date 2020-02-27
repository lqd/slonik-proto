import statistics
import time

import slonik_proto as slonik

techniques = [
    'python-fn',
    'ffi-pyo3',
    'ffi-pyo3-batch',
]

iterations = 2
calls = 10_000_000

def python_fn(arg):
    return arg

rust_no_op = slonik.no_op
rust_batch_no_op = slonik.batch_no_op

for technique in techniques:
    times = [None] * iterations

    print(f'benchmarking "{technique}" {iterations} times')
    for iteration in range(iterations):
        if technique == 'python-fn':
            start = time.perf_counter()
            for i in range(calls):
                res = python_fn(i)
        elif technique == 'ffi-pyo3':
            start = time.perf_counter()
            for i in range(calls):
                res = rust_no_op(i)           
        else:
            start = time.perf_counter()
            res = rust_batch_no_op(calls)

        times[iteration] = time.perf_counter() - start
        assert res == calls - 1 
        

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

