# cython: boundscheck=False, wraparound=False
"""CPython-native (Cython -> C extension) page fetcher: http.client + orjson,
tuple construction in typed C loops."""
import http.client
from urllib.parse import urlsplit

import orjson


def fetch_tuples(str url, long long offset, long long limit):
    cdef object parts = urlsplit(url)
    cdef list recs, out
    cdef dict r
    cdef Py_ssize_t i, n

    conn = http.client.HTTPConnection(parts.hostname, parts.port)
    try:
        conn.request("GET", f"/records?offset={offset}&limit={limit}")
        body = conn.getresponse().read()
    finally:
        conn.close()
    recs = orjson.loads(body)
    n = len(recs)
    out = [None] * n
    for i in range(n):
        r = recs[i]
        out[i] = (r["id"], r["value"], r["name"], r["ts"])
    return out


import gzip
import re

_LOG_RE = re.compile(r'^(\S+) \S+ \S+ \[(\d+)\] "(\w+) (\S+) [^"]*" (\d{3}) (\d+)$')


def parse_logs_tuples(str path):
    cdef list out = []
    cdef object match = _LOG_RE.match
    cdef object m
    with gzip.open(path, "rt") as f:
        text = f.read()
    for line in text.split("\n"):
        if not line:
            continue
        m = match(line)
        out.append((m[1], int(m[2]), m[3], m[4], int(m[5]), int(m[6])))
    return out


def write_rows(rows, str path):
    cdef long long n = 0
    cdef object dumps = orjson.dumps
    with open(path, "ab") as f:
        for r in rows:
            f.write(dumps(r.asDict()))
            f.write(b"\n")
            n += 1
    return n


def decode_messages_tuples(list msgs):
    cdef list out = []
    cdef list recs
    cdef dict r
    cdef object loads = orjson.loads
    for m in msgs:
        recs = loads(m)
        for r in recs:
            out.append((r["id"], r["value"], r["name"], r["ts"]))
    return out
