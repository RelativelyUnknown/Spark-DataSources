"""Pure-Python fetchers: urllib + json (rows) and urllib + json -> pyarrow (Arrow)."""
import json
from urllib.request import urlopen

import pyarrow as pa

ARROW_SCHEMA = pa.schema(
    [("id", pa.int64()), ("value", pa.float64()), ("name", pa.string()), ("ts", pa.int64())]
)


def _get(url, offset, limit):
    with urlopen(f"{url}/records?offset={offset}&limit={limit}") as r:
        return json.loads(r.read())


def fetch_tuples(url, offset, limit):
    return [(r["id"], r["value"], r["name"], r["ts"]) for r in _get(url, offset, limit)]


def fetch_batch(url, offset, limit):
    recs = _get(url, offset, limit)
    return pa.RecordBatch.from_arrays(
        [pa.array([r[k] for r in recs], type=ARROW_SCHEMA.field(k).type) for k in ARROW_SCHEMA.names],
        schema=ARROW_SCHEMA,
    )


# ---------------------------------------------------------------- compressed access logs
import gzip  # noqa: E402
import re  # noqa: E402

_LOG_RE = re.compile(r'^(\S+) \S+ \S+ \[(\d+)\] "(\w+) (\S+) [^"]*" (\d{3}) (\d+)$')
LOG_ARROW_SCHEMA = pa.schema([
    ("ip", pa.string()), ("ts", pa.int64()), ("method", pa.string()),
    ("path", pa.string()), ("status", pa.int32()), ("bytes", pa.int64())])


def parse_logs_tuples(path):
    out = []
    with gzip.open(path, "rt") as f:
        for line in f:
            m = _LOG_RE.match(line.rstrip("\n"))
            out.append((m[1], int(m[2]), m[3], m[4], int(m[5]), int(m[6])))
    return out


def parse_logs_batch(path):
    rows = parse_logs_tuples(path)
    cols = list(zip(*rows)) if rows else [()] * 6
    return pa.RecordBatch.from_arrays(
        [pa.array(c, type=t) for c, t in zip(cols, LOG_ARROW_SCHEMA.types)], schema=LOG_ARROW_SCHEMA)


# ---------------------------------------------------------------- NDJSON writers
def write_rows(rows, path):
    n = 0
    with open(path, "a") as f:
        for r in rows:
            f.write(json.dumps(r.asDict(), separators=(",", ":")) + "\n")
            n += 1
    return n


def write_batch(batch, path):
    with open(path, "a") as f:
        for d in batch.to_pylist():
            f.write(json.dumps(d, separators=(",", ":")) + "\n")
    return batch.num_rows


# ---------------------------------------------------------------- MQTT payload decoding
def decode_messages_tuples(msgs):
    return [(r["id"], r["value"], r["name"], r["ts"]) for m in msgs for r in json.loads(m)]
