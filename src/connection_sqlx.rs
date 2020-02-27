use sqlx::postgres::PgRow;
use sqlx::row::Row;
use sqlx::PgPool;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::{deserialize_bytes_via_python, executor};

/// An asynchronous PG driver using `sqlx` with the "slonik-rt" async runtime bridge
/// to python's `asyncio`.
#[pyclass]
pub struct SqlxConnection {
    pool: *const PgPool,
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

        SqlxConnection { pool }
    }

    /// Spawns an async task to execute the given SQL query, bridged to python via 
    /// the completion callback and IO interest registration.
    /// As `sqlx` doesn't yet provide a way to access the query's result column types,
    /// the client has to provide them via the `columns` vec.
    fn query(
        &self,
        query: &'static str,
        columns: Vec<String>,
        on_done_callback: PyObject,
        read_registrar: PyObject,
        write_registrar: PyObject,
    ) {
        let pool = {
            let pool = unsafe { Arc::from_raw(self.pool) };
            let clone = Arc::clone(&pool);
            std::mem::forget(pool);
            clone
        };

        executor::spawn_for_python(
            do_query(pool, query, columns),
            on_done_callback,
            read_registrar,
            write_registrar,
        );
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
