"""Benchmark harness shared by the CLI (run_bench.py) and the marimo notebook."""
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import mock_server  # noqa: E402

from spark_datasources import SOURCES, register_all  # noqa: E402
from spark_datasources.base import SCHEMA  # noqa: F401,E402
import importlib  # noqa: E402

FETCHERS = {
    "http_py": ("spark_datasources.pure_python", "fetch_tuples"),
    "http_py_arrow": ("spark_datasources.pure_python", "fetch_batch"),
    "http_rust": ("sparkhttp_rs", "fetch_tuples"),
    "http_rust_arrow": ("sparkhttp_rs", "fetch_batch"),
    "http_cython": ("sparkhttp_cy", "fetch_tuples"),
}
LABELS = {
    "http_py": "Python (json, tuples)",
    "http_py_arrow": "Python (json -> Arrow)",
    "http_rust": "Rust (tuples)",
    "http_rust_arrow": "Rust (Arrow, zero-copy)",
    "http_cython": "Cython + orjson (tuples)",
}


def get_spark(cores=4):
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master(f"local[{cores}]").appName("bench")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    register_all(spark)
    return spark


def _time(fn, repeats, warmup=1):
    for _ in range(warmup):
        fn()
    ts = []
    for _ in range(repeats):
        t = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t)
    return ts


def micro(url, page_size=50_000, repeats=5):
    """Fetch+parse a single page outside Spark (isolates the language/boundary cost)."""
    out = []
    for name, (mod, attr) in FETCHERS.items():
        if name not in SOURCES:
            continue
        fn = getattr(importlib.import_module(mod), attr)
        ts = _time(lambda: fn(url, 0, page_size), repeats)
        out.append(dict(arm=name, label=LABELS[name], median_s=statistics.median(ts),
                        rows=page_size, rows_per_s=page_size / statistics.median(ts)))
    return out


def spark_bench(spark, url, total_rows=1_000_000, partitions=4, page_size=50_000,
                actions=("scan", "toPandas"), repeats=3):
    """End-to-end through Spark. `scan` = full read + sum (no transfer of rows to driver),
    `toPandas` = full read + Arrow transfer to the driver."""
    out = []
    for name in FETCHERS:
        if name not in SOURCES:
            continue

        def df():
            return (spark.read.format(name).option("url", url).option("total_rows", total_rows)
                    .option("partitions", partitions).option("page_size", page_size).load())

        acts = {"scan": lambda: df().selectExpr("sum(value)", "count(*)").collect(),
                "toPandas": lambda: df().toPandas()}
        for a in actions:
            ts = _time(acts[a], repeats)
            out.append(dict(arm=name, label=LABELS[name], action=a, total_rows=total_rows,
                            partitions=partitions, median_s=statistics.median(ts),
                            min_s=min(ts), rows_per_s=total_rows / statistics.median(ts)))
    return out


def start_server():
    return mock_server.start()


# ------------------------------------------------------------------ logs + writers
import tempfile  # noqa: E402

import gen_logs  # noqa: E402
from spark_datasources import group  # noqa: E402

LABELS.update({
    "logs_py": "Python re (tuples)", "logs_py_arrow": "Python re -> Arrow",
    "logs_rust": "Rust regex (tuples)", "logs_rust_arrow": "Rust regex (Arrow)",
    "logs_cython": "Cython + re (tuples)",
    "ndjson_py": "Python json (Rows)", "ndjson_py_arrow": "Python json (Arrow -> dicts)",
    "ndjson_rust_arrow": "Rust arrow-json (Arrow)", "ndjson_cython": "Cython + orjson (Rows)",
})


def make_logs(files=4, lines=250_000, directory=None):
    return gen_logs.generate(directory or os.path.join(tempfile.gettempdir(), f"sds_logs_{files}x{lines}"),
                             files, lines)


def logs_bench(spark, directory, repeats=3):
    out = []
    for name in group("logs_"):
        fn = lambda: (spark.read.format(name).option("path", directory).load()
                      .selectExpr("count(*)", "sum(bytes)").collect())
        total = spark.read.format(name).option("path", directory).load().count()
        ts = _time(fn, repeats)
        out.append(dict(arm=name, label=LABELS[name], rows=total, median_s=statistics.median(ts),
                        min_s=min(ts), rows_per_s=total / statistics.median(ts)))
    return out


def writer_bench(spark, url, total_rows=500_000, partitions=4, repeats=3):
    df = (spark.read.format("http_rust_arrow").option("url", url).option("total_rows", total_rows)
          .option("partitions", partitions).option("page_size", 50_000).load())
    # materialise the input once so only the write path is measured
    src = df.cache()
    src.count()
    out = []
    for name in group("ndjson_"):
        def fn():
            d = tempfile.mkdtemp(prefix="sds_out_")
            src.write.format(name).option("path", d).mode("append").save()
        ts = _time(fn, repeats)
        out.append(dict(arm=name, label=LABELS[name], rows=total_rows, median_s=statistics.median(ts),
                        min_s=min(ts), rows_per_s=total_rows / statistics.median(ts)))
    src.unpersist()
    return out


# ------------------------------------------------------------------ MQTT streaming
import time  # noqa: E402

import mqtt_broker  # noqa: E402

LABELS.update({"mqtt_py": "Python json", "mqtt_rust": "Rust serde_json", "mqtt_cython": "Cython + orjson"})


def mqtt_bench(spark, n_msgs=300, recs_per_msg=1000, port=18832, repeats=2):
    """Time from first publish until every record has landed in a memory sink (broker included)."""
    mqtt_broker.start(port)
    out = []
    for name in group("mqtt_"):
        times = []
        for rep in range(repeats):
            topic, qn = f"bench/{name}/{rep}", f"b_{name}_{rep}"
            q = (spark.readStream.format(name).option("port", port).option("topic", topic).load()
                 .writeStream.format("memory").queryName(qn)
                 .trigger(processingTime="100 milliseconds").start())
            time.sleep(1.5)
            t = time.perf_counter()
            total = mqtt_broker.publish(port, topic, n_msgs, recs_per_msg)
            while spark.sql(f"select count(*) c from {qn}").first().c < total:
                time.sleep(0.05)
            times.append(time.perf_counter() - t)
            q.stop()
        out.append(dict(arm=name, label=LABELS[name], rows=total, median_s=statistics.median(times),
                        min_s=min(times), rows_per_s=total / statistics.median(times)))
    return out
