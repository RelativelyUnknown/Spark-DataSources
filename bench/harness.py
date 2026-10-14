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


# ------------------------------------------------------------------ Parquet
LABELS.update({"parquet_builtin": "Spark built-in parquet", "parquet_pyarrow": "pyarrow.parquet source",
               "parquet_rust": "Rust parquet source"})

# (aggregate expressions, DDL schema handed to the data-source arms; None = full file schema).
# The built-in reader prunes columns from the query by itself; Python data sources only prune
# when the user supplies `.schema(...)`.
PQ_QUERIES = {
    "full scan (all columns)": (["sum(id)", "sum(i)", "sum(d)", "sum(length(s))", "count(b)", "sum(dec)",
                                 "max(dt)", "max(ts)"], None),
    "one column": (["sum(d)"], "d double"),
    "count(*)": (["count(*)"], "id long"),
}


def make_parquet(spark, rows=10_000_000, files=8, directory=None):
    """Written by Spark with all defaults (snappy, INT96 timestamps, 128MB row groups)."""
    d = directory or os.path.join(tempfile.gettempdir(), f"sds_pq_{rows}_{files}")
    if not os.path.exists(d):
        (spark.range(rows).selectExpr(
            "id", "cast(id % 100 as int) i", "id / 8 d", "concat('user-', id % 100000) s",
            "id % 2 = 0 b", "cast(id as decimal(18,2)) dec", "date_add(date'2020-01-01', cast(id % 1000 as int)) dt",
            "timestamp'2024-01-01 00:00:00' + make_interval(0,0,0,0,0,0,id % 100000000) ts")
         .repartition(files).write.parquet(d))
    return d


def parquet_bench(spark, directory, repeats=3):
    out = []
    n = spark.read.parquet(directory).count()
    for name in ("parquet_builtin", "parquet_pyarrow", "parquet_rust"):
        for q, (expr, ddl) in PQ_QUERIES.items():
            def df():
                if name == "parquet_builtin":
                    return spark.read.parquet(directory)
                r = spark.read.format(name).option("path", directory)
                return (r.schema(ddl) if ddl else r).load()
            nparts = df().rdd.getNumPartitions()
            ts = _time(lambda: df().selectExpr(*expr).collect(), repeats)
            out.append(dict(arm=name, label=LABELS[name], query=q, rows=n, partitions=nparts,
                            median_s=statistics.median(ts), min_s=min(ts), rows_per_s=n / statistics.median(ts)))
    return out


def parquet_micro(directory, repeats=5):
    """Decode every row group of one file outside Spark (pure decode cost, includes Arrow hand-off)."""
    import glob as _g

    from spark_datasources.parquet_source import ENGINES

    path = sorted(_g.glob(os.path.join(directory, "*.parquet")))[0]
    out = []
    for engine, label in (("pyarrow", LABELS["parquet_pyarrow"]), ("rust", LABELS["parquet_rust"])):
        inspect, batches = ENGINES[engine]
        _, rgs = inspect(path)
        n = sum(r for _, _, r in rgs)
        ts = _time(lambda: sum(b.num_rows for b in batches(path, [i for i, _, _ in rgs])), repeats)
        out.append(dict(arm="parquet_" + engine, label=label, rows=n, median_s=statistics.median(ts),
                        rows_per_s=n / statistics.median(ts)))
    return out


# ------------------------------------------------------------------ concurrent fan-out
LABELS.update({"fanout_py_seq": "Python sequential", "fanout_py_threads": "Python threads",
               "fanout_rust_seq": "Rust sequential", "fanout_rust_threads": "Rust threads",
               "fanout_rust_async": "Rust async (tokio)"})
FANOUT_ARMS = ("fanout_py_seq", "fanout_py_threads", "fanout_rust_seq", "fanout_rust_threads", "fanout_rust_async")


def fanout_probe(url, pages=100, page_size=1000, latencies=(5, 20, 50), concurrencies=(8, 64, 512)):
    """No Spark: one partition's worth of pages per call. Rows: arm, latency_ms, concurrency, median_s."""
    from spark_datasources.fanout import FETCHERS

    offs = list(range(0, pages * page_size, page_size))
    FETCHERS["fanout_rust_async"](url, offs[:8], page_size, 8, "")
    out = []
    for lat in latencies:
        extra = f"&delay_ms={lat}"
        for arm in ("fanout_py_seq", "fanout_rust_seq"):
            ts = _time(lambda: FETCHERS[arm](url, offs, page_size, 1, extra), 1, warmup=0)
            out.append(dict(arm=arm, label=LABELS[arm], latency_ms=lat, concurrency=1,
                            median_s=ts[0], pages_per_s=pages / ts[0]))
        for conc in concurrencies:
            for arm in ("fanout_py_threads", "fanout_rust_threads", "fanout_rust_async"):
                ts = _time(lambda: FETCHERS[arm](url, offs, page_size, conc, extra), 2, warmup=0)
                m = statistics.median(ts)
                out.append(dict(arm=arm, label=LABELS[arm], latency_ms=lat, concurrency=conc,
                                median_s=m, pages_per_s=pages / m))
    return out


def server_capacity(url, n=3000):
    from spark_datasources.fanout import FETCHERS

    s = time.perf_counter()
    FETCHERS["fanout_rust_async"](url, list(range(0, n * 100, 100)), 100, 512, "")
    return n / (time.perf_counter() - s)


def fanout_bench(spark, url, total_rows=400_000, partitions=4, page_size=1000, latency_ms=20,
                 concurrency=64, repeats=2):
    """Through Spark: each of `partitions` tasks fetches total_rows/partitions/page_size pages."""
    out = []
    for name in FANOUT_ARMS:
        def run():
            return (spark.read.format(name).option("url", url).option("total_rows", total_rows)
                    .option("partitions", partitions).option("page_size", page_size)
                    .option("latency_ms", latency_ms).option("concurrency", concurrency)
                    .load().selectExpr("count(*)", "sum(value)").collect())
        ts = _time(run, repeats, warmup=1)
        out.append(dict(arm=name, label=LABELS[name], rows=total_rows, latency_ms=latency_ms,
                        concurrency=concurrency, median_s=statistics.median(ts), min_s=min(ts),
                        rows_per_s=total_rows / statistics.median(ts)))
    return out
