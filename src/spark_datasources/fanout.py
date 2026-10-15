"""Latency-bound fan-out: each Spark partition fetches many small pages. All arms return Arrow
batches in page order, so only the I/O strategy differs.

Options: url, total_rows, partitions, page_size, latency_ms (server-side delay), concurrency.
Arms:  py_seq (urllib, one at a time) | py_threads (ThreadPoolExecutor) |
       rust_seq (ureq) | rust_threads (OS threads + ureq) | rust_async (tokio + reqwest, `buffered(N)`).
"""
import json
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen

import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader

from .base import SCHEMA, PageRange
from .pure_python import ARROW_SCHEMA


def _py_page(url, offset, limit, extra):
    with urlopen(f"{url}/records?offset={offset}&limit={limit}{extra}") as r:
        recs = json.loads(r.read())
    return pa.RecordBatch.from_arrays(
        [pa.array([x[k] for x in recs], type=ARROW_SCHEMA.field(k).type) for k in ARROW_SCHEMA.names],
        schema=ARROW_SCHEMA)


def py_seq(url, offsets, limit, concurrency, extra):
    return [_py_page(url, o, limit, extra) for o in offsets]


def py_threads(url, offsets, limit, concurrency, extra):
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        return list(ex.map(lambda o: _py_page(url, o, limit, extra), offsets))


def _rust(mode):
    def run(url, offsets, limit, concurrency, extra):
        import sparkhttp_rs

        return sparkhttp_rs.fanout_batches(url, list(offsets), limit, concurrency, mode, extra)

    run.__name__ = f"rust_{mode}"
    return run


FETCHERS = {
    "fanout_py_seq": py_seq,
    "fanout_py_threads": py_threads,
    "fanout_rust_seq": _rust("seq"),
    "fanout_rust_threads": _rust("threads"),
    "fanout_rust_async": _rust("async"),
}


class FanoutReader(DataSourceReader):
    def __init__(self, options, fetch):
        self.url = options["url"].rstrip("/")
        self.total = int(options.get("total_rows", 100_000))
        self.nparts = int(options.get("partitions", 4))
        self.page = int(options.get("page_size", 1000))
        self.conc = int(options.get("concurrency", 64))
        self.extra = f"&delay_ms={options['latency_ms']}" if options.get("latency_ms") else ""
        self.fetch = fetch

    def partitions(self):
        step = -(-self.total // self.nparts)
        return [PageRange(s, min(s + step, self.total)) for s in range(0, self.total, step)]

    def read(self, partition):
        offsets = list(range(partition.start, partition.stop, self.page))
        # last page may be short; keep the contract of the plain HTTP source
        limit = self.page
        batches = self.fetch(self.url, offsets, limit, self.conc, self.extra)
        for b, off in zip(batches, offsets):
            yield b if off + limit <= partition.stop else b.slice(0, partition.stop - off)


def make_fanout_source(name, fetch):
    class _Source(DataSource):
        @classmethod
        def name(cls):
            return name

        def schema(self):
            return SCHEMA

        def reader(self, schema):
            return FanoutReader(self.options, fetch)

    _Source.__name__ = f"Fanout_{name}"
    return _Source
