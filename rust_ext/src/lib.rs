//! HTTP + JSON page fetcher exposed to Python in two flavours:
//! `fetch_tuples` (list of Python tuples) and `fetch_batch` (zero-copy Arrow RecordBatch).
//! Network I/O and JSON parsing run with the GIL released.
use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Float64Builder, Int32Builder, Int64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::pyarrow::{PyArrowType, ToPyArrow};
use flate2::read::GzDecoder;
use regex::Regex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
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

// ---------------------------------------------------------------- compressed access logs
static LOG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^(\S+) \S+ \S+ \[(\d+)\] "(\w+) (\S+) [^"]*" (\d{3}) (\d+)$"#).unwrap()
});

type LogRow = (String, i64, String, String, i32, i64);

fn read_gz(path: &str) -> Result<String, String> {
    let mut s = String::new();
    GzDecoder::new(BufReader::new(File::open(path).map_err(|e| e.to_string())?))
        .read_to_string(&mut s)
        .map_err(|e| e.to_string())?;
    Ok(s)
}

fn parse_logs(path: &str) -> Result<Vec<LogRow>, String> {
    let text = read_gz(path)?;
    let mut out = Vec::new();
    for line in text.lines() {
        let c = LOG_RE.captures(line).ok_or_else(|| format!("bad line: {line}"))?;
        out.push((
            c[1].to_string(),
            c[2].parse().unwrap(),
            c[3].to_string(),
            c[4].to_string(),
            c[5].parse().unwrap(),
            c[6].parse().unwrap(),
        ));
    }
    Ok(out)
}

#[pyfunction]
fn parse_logs_tuples(py: Python<'_>, path: &str) -> PyResult<Vec<LogRow>> {
    py.detach(|| parse_logs(path)).map_err(err)
}

#[pyfunction]
fn parse_logs_batch<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let batch = py
        .detach(|| {
            let rows = parse_logs(path)?;
            let n = rows.len();
            let mut ip = StringBuilder::with_capacity(n, n * 12);
            let mut ts = Int64Builder::with_capacity(n);
            let mut method = StringBuilder::with_capacity(n, n * 4);
            let mut p = StringBuilder::with_capacity(n, n * 16);
            let mut status = Int32Builder::with_capacity(n);
            let mut bytes = Int64Builder::with_capacity(n);
            for r in &rows {
                ip.append_value(&r.0);
                ts.append_value(r.1);
                method.append_value(&r.2);
                p.append_value(&r.3);
                status.append_value(r.4);
                bytes.append_value(r.5);
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("ip", DataType::Utf8, true),
                Field::new("ts", DataType::Int64, true),
                Field::new("method", DataType::Utf8, true),
                Field::new("path", DataType::Utf8, true),
                Field::new("status", DataType::Int32, true),
                Field::new("bytes", DataType::Int64, true),
            ]));
            let cols: Vec<ArrayRef> = vec![
                Arc::new(ip.finish()),
                Arc::new(ts.finish()),
                Arc::new(method.finish()),
                Arc::new(p.finish()),
                Arc::new(status.finish()),
                Arc::new(bytes.finish()),
            ];
            RecordBatch::try_new(schema, cols).map_err(|e| e.to_string())
        })
        .map_err(err)?;
    batch.to_pyarrow(py)
}

// ---------------------------------------------------------------- NDJSON writer
/// Append an Arrow batch to `path` as newline-delimited JSON; returns rows written.
#[pyfunction]
fn write_ndjson(py: Python<'_>, batch: PyArrowType<RecordBatch>, path: &str) -> PyResult<usize> {
    let batch = batch.0;
    py.detach(|| {
        let f = OpenOptions::new().create(true).append(true).open(path).map_err(|e| e.to_string())?;
        let mut w = arrow::json::LineDelimitedWriter::new(BufWriter::new(f));
        w.write(&batch).map_err(|e| e.to_string())?;
        w.finish().map_err(|e| e.to_string())?;
        Ok::<_, String>(batch.num_rows())
    })
    .map_err(err)
}

// ---------------------------------------------------------------- MQTT payload decoding
/// Decode a list of JSON-array payloads (bytes) into row tuples; GIL released while parsing.
#[pyfunction]
fn decode_messages_tuples(
    py: Python<'_>,
    msgs: &Bound<'_, PyList>,
) -> PyResult<Vec<(i64, f64, String, i64)>> {
    let msgs: Vec<Vec<u8>> = msgs
        .iter()
        .map(|m| Ok(m.cast::<PyBytes>()?.as_bytes().to_vec()))
        .collect::<PyResult<_>>()?;
    py.detach(|| {
        let mut out = Vec::new();
        for m in &msgs {
            let recs: Vec<Rec> = serde_json::from_slice(m).map_err(|e| e.to_string())?;
            out.extend(recs.into_iter().map(|r| (r.id, r.value, r.name.into_owned(), r.ts)));
        }
        Ok::<_, String>(out)
    })
    .map_err(err)
}

#[pymodule]
fn sparkhttp_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fetch_tuples, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_batch, m)?)?;
    m.add_function(wrap_pyfunction!(parse_logs_tuples, m)?)?;
    m.add_function(wrap_pyfunction!(parse_logs_batch, m)?)?;
    m.add_function(wrap_pyfunction!(write_ndjson, m)?)?;
    m.add_function(wrap_pyfunction!(decode_messages_tuples, m)?)?;
    Ok(())
}
