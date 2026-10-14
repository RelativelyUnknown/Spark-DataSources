"""Native-library enrichment: lat/lng -> H3 cell, text -> xxh3-64 and Unicode word count.

Input points are generated identically for every arm (vectorised numpy/pyarrow, so input cost is shared and
small); the arms differ only in how the three enrichment columns are computed:
  enrich_py        per-row Python loop over the C bindings `h3` + `xxhash` + `re`, yields tuples
  enrich_py_arrow  same per-row loop, but assembles Arrow columns
  enrich_rust      one Arrow batch in -> Arrow batch out, `h3o` + `xxhash-rust` + `unicode-segmentation`

Options: total_rows, partitions, batch_rows, resolution (H3, default 9).
"""
import re

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql.datasource import DataSource, DataSourceReader

from .base import PageRange

SCHEMA = "id long, lat double, lng double, h3 long, hash long, words int"
OUT = pa.schema([("id", pa.int64()), ("lat", pa.float64()), ("lng", pa.float64()), ("h3", pa.int64()),
                 ("hash", pa.int64()), ("words", pa.int32())])
_WORD = re.compile(r"\w+")


def make_points(start, stop):
    """Deterministic input: id, lat, lng, text (contains non-ASCII so Unicode handling is exercised)."""
    i = np.arange(start, stop, dtype=np.int64)
    lat = -60.0 + (i * 7919 % 12000) / 100.0
    lng = -180.0 + (i * 104729 % 36000) / 100.0
    ids = pa.array(i)
    text = pc.binary_join_element_wise(
        pa.scalar("event"), ids.cast(pa.string()), pa.scalar("at node-"),
        pa.array(i % 97).cast(pa.string()), pa.scalar("café ok"), pa.scalar(" "))
    return pa.RecordBatch.from_arrays([ids, pa.array(lat), pa.array(lng), text],
                                      names=["id", "lat", "lng", "text"])


def _py_columns(batch, res):
    from h3.api import basic_int as h3i
    import xxhash

    cell, xxh, mask = h3i.latlng_to_cell, xxhash.xxh3_64_intdigest, (1 << 63)
    ids, lat, lng, text = (batch.column(k).to_pylist() for k in range(4))
    h3 = [cell(a, b, res) for a, b in zip(lat, lng)]
    hs = [(h - (1 << 64)) if h >= mask else h for h in (xxh(t.encode()) for t in text)]
    words = [len(_WORD.findall(t)) for t in text]
    return ids, lat, lng, h3, hs, words


def enrich_py(batch, res):
    return list(zip(*_py_columns(batch, res)))


def enrich_py_arrow(batch, res):
    cols = _py_columns(batch, res)
    return pa.RecordBatch.from_arrays([pa.array(c, type=t) for c, t in zip(cols, OUT.types)], schema=OUT)


def enrich_rust(batch, res):
    import sparkhttp_rs

    return sparkhttp_rs.enrich_batch(batch, res)


ENRICHERS = {"enrich_py": enrich_py, "enrich_py_arrow": enrich_py_arrow, "enrich_rust": enrich_rust}


class EnrichReader(DataSourceReader):
    def __init__(self, options, fn):
        self.total = int(options.get("total_rows", 1_000_000))
        self.nparts = int(options.get("partitions", 4))
        self.batch = int(options.get("batch_rows", 50_000))
        self.res = int(options.get("resolution", 9))
        self.fn = fn

    def partitions(self):
        step = -(-self.total // self.nparts)
        return [PageRange(s, min(s + step, self.total)) for s in range(0, self.total, step)]

    def read(self, partition):
        for off in range(partition.start, partition.stop, self.batch):
            yield from self._one(make_points(off, min(off + self.batch, partition.stop)))

    def _one(self, batch):
        res = self.fn(batch, self.res)
        if isinstance(res, list):
            yield from res
        else:
            yield res


def make_enrich_source(name, fn):
    class _Source(DataSource):
        @classmethod
        def name(cls):
            return name

        def schema(self):
            return SCHEMA

        def reader(self, schema):
            return EnrichReader(self.options, fn)

    _Source.__name__ = f"Enrich_{name}"
    return _Source
