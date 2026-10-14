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
