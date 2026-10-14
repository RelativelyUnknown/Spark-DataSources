import importlib

from .base import make_log_source, make_source, make_writer_source
from .mqtt import make_mqtt_source
from .parquet_source import make_parquet_source

PY = "spark_datasources.pure_python"
# registered name -> (factory, module, attr[, extra]); resolved lazily so a missing native build
# only disables that arm
_SPECS = {
    # HTTP + JSON reader
    "http_py": (make_source, PY, "fetch_tuples"),
    "http_py_arrow": (make_source, PY, "fetch_batch"),
    "http_rust": (make_source, "sparkhttp_rs", "fetch_tuples"),
    "http_rust_arrow": (make_source, "sparkhttp_rs", "fetch_batch"),
    "http_cython": (make_source, "sparkhttp_cy", "fetch_tuples"),
    # gzip access-log reader
    "logs_py": (make_log_source, PY, "parse_logs_tuples"),
    "logs_py_arrow": (make_log_source, PY, "parse_logs_batch"),
    "logs_rust": (make_log_source, "sparkhttp_rs", "parse_logs_tuples"),
    "logs_rust_arrow": (make_log_source, "sparkhttp_rs", "parse_logs_batch"),
    "logs_cython": (make_log_source, "sparkhttp_cy", "parse_logs_tuples"),
    # NDJSON writers
    "ndjson_py": (lambda n, f: make_writer_source(n, f, False), PY, "write_rows"),
    "ndjson_py_arrow": (lambda n, f: make_writer_source(n, f, True), PY, "write_batch"),
    "ndjson_rust_arrow": (lambda n, f: make_writer_source(n, f, True), "sparkhttp_rs", "write_ndjson"),
    "ndjson_cython": (lambda n, f: make_writer_source(n, f, False), "sparkhttp_cy", "write_rows"),
    # MQTT streaming readers (decode only; simple stream reader returns rows)
    "mqtt_py": (make_mqtt_source, PY, "decode_messages_tuples"),
    "mqtt_rust": (make_mqtt_source, "sparkhttp_rs", "decode_messages_tuples"),
    "mqtt_cython": (make_mqtt_source, "sparkhttp_cy", "decode_messages_tuples"),
}
SOURCES = {}
# parquet: (name, engine); engine rust needs the native module, resolved at read time
for _n, _e in (("parquet_rust", "rust"), ("parquet_pyarrow", "pyarrow")):
    SOURCES[_n] = make_parquet_source(_n, _e)
for _name, (_factory, _mod, _attr) in _SPECS.items():
    try:
        SOURCES[_name] = _factory(_name, getattr(importlib.import_module(_mod), _attr))
    except ImportError:
        pass


def group(prefix):
    return sorted(n for n in SOURCES if n.startswith(prefix))


def register_all(spark):
    for cls in SOURCES.values():
        spark.dataSource.register(cls)
    return sorted(SOURCES)
