use sqlx::postgres::PgRow;
use sqlx::row::Row;
use sqlx::PgPool;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::{deserialize_bytes_via_python, executor};

#[cfg(feature = "cheating")]
use std::collections::HashMap;
#[cfg(feature = "cheating")]
use std::sync::Mutex;

/// An asynchronous PG driver using `sqlx` with the "slonik-rt" async runtime bridge
/// to python's `asyncio`.
#[cfg(not(feature = "cheating"))]
#[pyclass]
pub struct SqlxConnection {
    pool: *const PgPool,
}

/// An asynchronous PG driver using `sqlx` with the "slonik-rt" async runtime bridge
/// to python's `asyncio`. 
/// Contains a terrible infinitely growing cache to see how caching affects benchmarks.
#[cfg(feature = "cheating")]
#[pyclass]
pub struct SqlxConnection {
    pool: *const PgPool,
    cache: Arc<Mutex<HashMap<&'static str, PyObject>>>,
}

impl Drop for SqlxConnection {
    fn drop(&mut self) {
        unsafe {
            Arc::from_raw(self.pool);
        }
    }
}

#[pymethods]
impl SqlxConnection {
    #[new]
    fn new(url: &str) -> Self {
        // Note: connecting to PG is an async operation in sql, but here we'll
        // do the same thing as for TcpStreams, block while connecting, but handle
        // queries asynchronously.
        use futures::executor;

        let builder = PgPool::builder().max_size(1); /* defaut max_size: 10 */
        let pool = builder.build(url);
        let pool = executor::block_on(pool).expect("Building pg connection pool failed");
        let pool = Arc::into_raw(Arc::new(pool));

        #[cfg(not(feature = "cheating"))]
        {
            SqlxConnection { pool }
        }

        #[cfg(feature = "cheating")]
        {
            let cache = Arc::new(Mutex::new(HashMap::default()));
            SqlxConnection { pool, cache }
        }
    }

    /// Spawns an async task to execute the given SQL query, bridged to python via 
    /// the completion callback and IO interest registration.
    /// As `sqlx` doesn't yet provide a way to access the query's result column types,
    /// the client has to provide them via the `columns` vec.
    fn query(
        &mut self,
        query: &'static str,
        columns: Vec<String>,
        on_done_callback: PyObject,
        read_registrar: PyObject,
        write_registrar: PyObject,
    ) {
        {
            #[cfg(feature = "cheating")]
            {
                let cache = self.cache.lock().unwrap();
                if let Some(rows) = cache.get(query) {
                    slonik_rt::execute_python_callback(&on_done_callback, rows);
                    return;
                }
            }
        }

        let pool = {
            let pool = unsafe { Arc::from_raw(self.pool) };
            let clone = Arc::clone(&pool);
            std::mem::forget(pool);
            clone
        };

        #[cfg(not(feature = "cheating"))]
        {
            let fut = do_query(pool, query, columns);
            executor::spawn_for_python(
                fut,
                on_done_callback,
                read_registrar,
                write_registrar,
            );
        }

        #[cfg(feature = "cheating")]
        {
            let cache = Arc::clone(&self.cache);

            let fut = async move {
                let rows = do_query(pool, query, columns).await;

                let gil = Python::acquire_gil();
                let py = gil.python();

                let rows = rows.to_object(py);
                let clone = rows.clone_ref(py);

                {
                    let mut cache = cache.lock().unwrap();
                    cache.insert(query, rows);
                }

                clone
            };

            executor::spawn_for_python(
                fut,
                on_done_callback,
                read_registrar,
                write_registrar,
            );
        }        
    }
}

// As `sqlx` doesn't provide access to this information, use a wrapper struct to deserialize
// columns types to python as a list of tuples.
struct SlonikRows {
    columns: Vec<String>,
    rows: Vec<PgRow>,
}

impl pyo3::ToPyObject for SlonikRows {
    fn to_object(&self, py: Python) -> PyObject {
        let mut rows = self.rows.iter().map(|row| {
            let values = self
                .columns
                .iter()
                .enumerate()
                .map(|(col_idx, col_ty)| deserialize_column(py, &row, col_ty, col_idx));
            PyTuple::new(py, values)
        });

        PyList::new(py, &mut rows).to_object(py)
    }
}

// Note: this fetches all results of a query eagerly
async fn do_query(pool: Arc<PgPool>, query: &'static str, columns: Vec<String>) -> SlonikRows {
    // println!("> starting query: {}", query);

    let mut conn = pool.as_ref();
    let rows = sqlx::query(query).fetch_all(&mut conn);
    let rows = rows.await.unwrap();

    let result = SlonikRows { columns, rows };

    // println!("> query done: {}", query);
    result
}

fn deserialize_column(py: Python<'_>, row: &PgRow, col_ty: &str, col_idx: usize) -> PyObject {
    match col_ty {
        "bool" | "BOOL" => row.get::<bool, _>(col_idx).to_object(py),

        "int2" | "INT2" => row.get::<i16, _>(col_idx).to_object(py),
        "int4" | "INT4" => row.get::<i32, _>(col_idx).to_object(py),
        "int8" | "INT8" => row.get::<i64, _>(col_idx).to_object(py),

        "float4" | "FLOAT4" => row.get::<f32, _>(col_idx).to_object(py),
        "float8" | "FLOAT8" => row.get::<f64, _>(col_idx).to_object(py),

        "text" | "unknown" | "bpchar" | "varchar" | "TEXT" | "UNKNOWN" | "BPCHAR" | "VARCHAR" => {
            row.get::<String, _>(col_idx).to_object(py)
        }

        "name" | "NAME" => row.get::<String, _>(col_idx).to_object(py),
        "oid" | "OID" => row.get::<i32, _>(col_idx).to_object(py),

        "json" | "jsonb" | "JSON" | "JSONB" => {
            let value = row.get::<Vec<u8>, _>(col_idx);
            let value = if col_ty == "json" {
                &value
            } else {
                &value[1..]
            };

            const DESERIALIZER: &str = r#"
import json
ret = json.loads(bytes(value))
"#;
            deserialize_bytes_via_python(py, value, DESERIALIZER)
        }

        "uuid" | "UUID" => {
            let value = row.get::<Vec<u8>, _>(col_idx);
            const DESERIALIZER: &str = r#"
import uuid
ret = uuid.UUID(bytes=bytes(value))
"#;
            deserialize_bytes_via_python(py, &value, DESERIALIZER)
        }

        _ => panic!("unknown type {:?}", col_ty),
    }
}
