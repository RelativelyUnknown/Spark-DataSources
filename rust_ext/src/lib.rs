//! HTTP + JSON page fetcher exposed to Python in two flavours:
//! `fetch_tuples` (list of Python tuples) and `fetch_batch` (zero-copy Arrow RecordBatch).
//! Network I/O and JSON parsing run with the GIL released.
use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Rec<'a> {
    id: i64,
    value: f64,
    #[serde(borrow)]
    name: Cow<'a, str>,
    ts: i64,
}

fn get_body(url: &str, offset: u64, limit: u64) -> Result<Vec<u8>, String> {
    let full = format!("{url}/records?offset={offset}&limit={limit}");
    let mut resp = ureq::get(&full).call().map_err(|e| e.to_string())?;
    resp.body_mut()
        .with_config()
        .limit(u64::MAX)
        .read_to_vec()
        .map_err(|e| e.to_string())
}

fn err(e: impl ToString) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

#[pyfunction]
fn fetch_tuples(
    py: Python<'_>,
    url: &str,
    offset: u64,
    limit: u64,
) -> PyResult<Vec<(i64, f64, String, i64)>> {
    py.detach(|| {
        let body = get_body(url, offset, limit)?;
        let recs: Vec<Rec> = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
        Ok::<_, String>(
            recs.into_iter()
                .map(|r| (r.id, r.value, r.name.into_owned(), r.ts))
                .collect(),
        )
    })
    .map_err(err)
}

fn build_batch(body: &[u8]) -> Result<RecordBatch, String> {
    let recs: Vec<Rec> = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let n = recs.len();
    let (mut id, mut val, mut ts) = (
        Int64Builder::with_capacity(n),
        Float64Builder::with_capacity(n),
        Int64Builder::with_capacity(n),
    );
    let mut name = StringBuilder::with_capacity(n, n * 12);
    for r in &recs {
        id.append_value(r.id);
        val.append_value(r.value);
        name.append_value(&r.name);
        ts.append_value(r.ts);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(id.finish()),
        Arc::new(val.finish()),
        Arc::new(name.finish()),
        Arc::new(ts.finish()),
    ];
    RecordBatch::try_new(schema, cols).map_err(|e| e.to_string())
}

#[pyfunction]
fn fetch_batch<'py>(
    py: Python<'py>,
    url: &str,
    offset: u64,
    limit: u64,
) -> PyResult<Bound<'py, PyAny>> {
    let batch = py
        .detach(|| build_batch(&get_body(url, offset, limit)?))
        .map_err(err)?;
    batch.to_pyarrow(py)
}

#[pymodule]
fn sparkhttp_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fetch_tuples, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_batch, m)?)?;
    Ok(())
}
