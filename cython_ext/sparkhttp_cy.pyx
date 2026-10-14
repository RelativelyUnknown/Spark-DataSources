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
