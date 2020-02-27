use postgres::rows::{Row, Rows};
use postgres::stmt;
use postgres::types::{self, Type};
use postgres::Connection;
use postgres::TlsMode;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::deserialize_bytes_via_python;

/// A synchronous PG driver using `rust-postgres`
#[pyclass]
pub struct RustPgConnection {
    connection: Connection,
}

#[pymethods]
impl RustPgConnection {
    #[new]
    fn new(url: &str) -> Self {
        let connection = Connection::connect(url, TlsMode::None).expect("Couldn't connect");
        RustPgConnection { connection }
    }

    fn prepare(&self, query: &'static str) -> Statement {
        let statement = self.connection.prepare(query).unwrap();
        let statement = Box::into_raw(Box::new(statement)) as *mut ();
        Statement { statement }
    }

    fn query<'p>(&self, py: Python<'p>, query: &str) -> PyResult<&'p PyList> {
        let rows = self
            .connection
            .query(query, &[])
            .expect("Couldn't execute query");
        let results = deserialize_rows(py, rows);
        Ok(results)
    }
}

#[pyclass]
struct Statement {
    statement: *mut (),
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.statement);
        }
    }
}

#[pymethods]
impl Statement {
    fn query<'p>(&self, py: Python<'p>) -> PyResult<&'p PyList> {
        let statement = self.statement as *const stmt::Statement;
        let statement = unsafe { statement.as_ref().unwrap() };
        let rows = statement
            .query(&[])
            .expect("Couldn't execute prepared statement");
        let results = deserialize_rows(py, rows);
        Ok(results)
    }
}

fn deserialize_rows<'p>(py: Python<'p>, rows: Rows) -> &'p PyList {
    let columns = rows.columns();
    let mut rows = rows.iter().map(|row| {
        let values = (0..columns.len()).map(|col_idx| {
            let col_ty = columns[col_idx].type_();
            deserialize_column(py, &row, col_ty, col_idx)
        });
        PyTuple::new(py, values)
    });
    PyList::new(py, &mut rows)
}

fn deserialize_column(py: Python<'_>, row: &Row, col_ty: &Type, col_idx: usize) -> PyObject {
    match *col_ty {
        types::BOOL => row.get::<_, bool>(col_idx).to_object(py),
        types::CHAR => row.get::<_, i8>(col_idx).to_object(py),
        
        types::INT2 => row.get::<_, i16>(col_idx).to_object(py),
        types::INT4 => row.get::<_, i32>(col_idx).to_object(py),
        types::INT8 => row.get::<_, i64>(col_idx).to_object(py),
        types::FLOAT4 => row.get::<_, f32>(col_idx).to_object(py),
        types::FLOAT8 => row.get::<_, f64>(col_idx).to_object(py),
        types::TEXT | types::UNKNOWN | types::BPCHAR | types::VARCHAR => {
            row.get::<_, String>(col_idx).to_object(py)
        }

        types::OID => row.get::<_, u32>(col_idx).to_object(py),
        types::NAME => {
            row.get::<_, String>(col_idx).to_object(py)
        }

        types::JSON | types::JSONB => {
            let value = if col_ty == &types::JSON {
                row.get_bytes(col_idx).expect("couldn't access bytes")
            } else {
                &row.get_bytes(col_idx).expect("couldn't access bytes")[1..]
            };

            let deserializer = r#"
import json
ret = json.loads(bytes(value))
"#;
            deserialize_bytes_via_python(py, value, deserializer)
        }

        types::UUID => {
            let value = row.get_bytes(col_idx).expect("couldn't access bytes");
            let deserializer = r#"
import uuid
ret = uuid.UUID(bytes=bytes(value))
"#;
            deserialize_bytes_via_python(py, value, deserializer)
        }

        _ => panic!("unknown type {:?}", col_ty),
    }
}
