#![feature(test)]

mod connection_rust_pg;
mod connection_sqlx;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;

use slonik_rt::net::TcpStream;
use slonik_rt::timer;
use slonik_rt::{executor, reactor};

use reactor::Interest;

extern crate test;

#[pyfunction]
pub fn no_op(arg: usize) -> usize {
    arg
}


#[inline(never)]
fn one() -> usize {
    test::black_box(1)
}

#[pyfunction]
pub fn batch_no_op(arg: usize) -> usize {
    // println!("calling one {} times", arg);

    for _ in 0..arg {
        one();
    }

    // return the last iteration counter as the other non-batched ops
    arg - 1
}

#[pymodule]
fn slonik_proto(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // Slonik module entry-points:
    // - one for the `rust-postgres` crate,
    // - and one for the `sqlx` crate
    m.add_class::<connection_rust_pg::RustPgConnection>()?;
    m.add_class::<connection_sqlx::SqlxConnection>()?;

    // asyncio registrar interface
    m.add_wrapped(wrap_pyfunction!(on_fd_read_ready))?;
    m.add_wrapped(wrap_pyfunction!(on_fd_write_ready))?;

    // Temporary examples
    m.add_wrapped(wrap_pyfunction!(sleep_example))?;
    m.add_wrapped(wrap_pyfunction!(async_add_example))?;
    m.add_wrapped(wrap_pyfunction!(async_io_example))?;
    m.add_wrapped(wrap_pyfunction!(async_sqlx_example))?;

    m.add_wrapped(wrap_pyfunction!(no_op))?;
    m.add_wrapped(wrap_pyfunction!(batch_no_op))?;

    Ok(())
}

/// Entry point callback, when the python asyncio reactor notifies the rust reactor
/// that the fd is ready to read
#[pyfunction]
fn on_fd_read_ready(_py: Python<'_>, fd: i32) {
    // println!("rust - received read ready event for fd {}", fd);
    reactor::on_fd_ready(fd, Interest::Readable);
}

/// Entry point callback, when the python asyncio reactor notifies the rust reactor
/// that the fd is ready to write
#[pyfunction]
fn on_fd_write_ready(_py: Python<'_>, fd: i32) {
    // println!("rust - received write ready event for fd {}", fd);
    reactor::on_fd_ready(fd, Interest::Writable);
}

// The `python_deserializer` expects statements deserializing a bytes `value`
// local, into a local named `ret`.
pub(crate) fn deserialize_bytes_via_python(
    py: Python<'_>,
    value: &[u8],
    python_deserializer: &str,
) -> PyObject {
    let locals = PyDict::new(py);
    locals
        .set_item("value", value)
        .expect("setting local bytes `value` failed");

    py.run(python_deserializer, None, Some(locals))
        .expect("eval error");
    let ret = locals.get_item("ret").expect("error getting `ret` local");
    ret.to_object(py)
}

// --- The following are the different examples -----

/// Example connecting to pg via sqlx and executing the query, which is expected to return
/// a single row, of one int2 column.
#[pyfunction]
fn async_sqlx_example(
    query: &'static str,
    on_done_callback: PyObject,
    read_registrar: PyObject,
    write_registrar: PyObject,
) {
    executor::spawn_for_python(
        async move {
            let ret = async_query(query).await;
            ret.expect("Query failed")
        },
        on_done_callback,
        read_registrar,
        write_registrar,
    );
}

async fn async_query(query: &str) -> Result<i32, Box<dyn std::error::Error>> {
    use sqlx::row::Row;
    use sqlx::PgPool;

    let builder = PgPool::builder().max_size(1); /* defaut max_size: 10 */
    let pool = builder
        .build("postgres://postgres@localhost:5433/slonik_test")
        .await?;

    let mut conn = &pool;
    let row = sqlx::query(query).fetch_one(&mut conn);
    let row = row.await?;

    let result: i32 = row.get(0);
    assert_eq!(pool.size(), 1);

    Ok(result)
}

/// Async sleep example: inefficiently do a python async sleep via a new rust thread
#[pyfunction]
fn sleep_example(_py: Python<'_>, delay: u64, on_done_callback: PyObject) {
    use std::thread;
    use std::time::Duration;

    thread::spawn(move || {
        let duration = Duration::from_millis(delay);
        thread::sleep(duration);
        slonik_rt::execute_python_callback(&on_done_callback, 29);
    });
}

/// Async example: with real `Future`s created by multiple `async fn` calls and `await` steps,
/// including a non-blocking sleep using an inefficient async timer
#[pyfunction]
fn async_add_example(py: Python<'_>, a: u32, b: u32, on_done_callback: PyObject) {
    executor::spawn_for_python(async_add(a, b), on_done_callback, py.None(), py.None());
}

async fn async_add(a: u32, b: u32) -> u32 {
    println!("before step_a: awaiting on trivially available result");
    let a = step_a(a).await;

    println!("after step_a ({}), before step_b", a);
    let b = step_b(b).await;

    println!("after step_b ({}), returning sum ({})", b, a + b);
    a + b
}

async fn step_a(a: u32) -> u32 {
    a
}

async fn step_b(b: u32) -> u32 {
    println!("step_b: awaiting on non-blocking sleep for 2s");
    timer::AsyncTimer::new(2000).await;
    println!("step_b: woken up after non-blocking sleep");

    b
}

/// Async IO example: reading google.com via a "slow server" with a custom delay
#[pyfunction]
pub fn async_io_example(
    _py: Python<'_>,
    delay: u32,
    on_done_callback: PyObject,
    read_registrar: PyObject,
    write_registrar: PyObject,
) {
    executor::spawn_for_python(
        async_io(delay),
        on_done_callback,
        read_registrar,
        write_registrar,
    );
}

async fn async_io(delay: u32) -> String {
    let addr = "http://www.google.com";

    println!("blocking TcpStream connect()");
    let conn =
        TcpStream::blocking_connect("slowwly.robertomurray.co.uk:80").expect("Connection failed");
    let res = delayed_http_get(conn, delay, addr)
        .await
        .expect("HTTP request failed");
    res
}

async fn delayed_http_get(
    mut conn: TcpStream,
    delay: u32,
    addr: &str,
) -> Result<String, std::io::Error> {
    use slonik_rt::{AsyncReadExt, AsyncWriteExt};

    let request = format!(
        "GET /delay/{}/url/{} HTTP/1.1\r\n\
         Host: slowwly.robertomurray.co.uk\r\n\
         Connection: close\r\n\
         \r\n",
        delay, addr,
    );

    println!("TcpStream connected -> non-blocking write of the HTTP GET request");
    let _ = conn.write_all(request.as_bytes()).await?;

    println!("HTTP GET sent -> non-blocking read of the response, in small chunks");
    let mut page = Vec::new();
    loop {
        // artificially small buffer to make more async/await trips to read the response
        let mut buf = vec![0; 128];

        let len = conn.read(&mut buf).await?;
        println!("read {} bytes from the stream", len);

        if len == 0 {
            break;
        }

        page.extend_from_slice(&buf[..len]);
    }

    println!("delayed HTTP GET complete");

    let page = String::from_utf8_lossy(&page).into();
    Ok(page)
}
