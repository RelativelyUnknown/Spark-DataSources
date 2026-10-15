"""Render bench/results.json into a self-contained animated HTML chart (docs/results.html)."""
import json
import sys

res = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "bench/results.json"))
FAN = {"fanout_py_seq": "py", "fanout_py_threads": "pya", "fanout_rust_seq": "rs",
       "fanout_rust_threads": "rsa", "fanout_rust_async": "va"}
ENR = {"enrich_py": "py", "enrich_py_arrow": "pya", "enrich_rust": "rsa"}
def KIND(a):
    """Series colour slot by implementation (entity), shared across panels where the entity is the same."""
    for table in (FAN, ENR):
        if a in table:
            return table[a]
    if a == "parquet_builtin":
        return "sp"
    if a == "parquet_pyarrow":
        return "pya"
    if a == "parquet_rust":
        return "rsa"
    if a in ("http_py", "logs_py", "ndjson_py", "mqtt_py"):
        return "py"
    if a.endswith("py_arrow"):
        return "pya"
    if a.endswith(("_rust", "mqtt_rust")):
        return "rs"
    if a.endswith("rust_arrow"):
        return "rsa"
    return "cy"


panels = []
def add(title, sub, rows, base, key="rows_per_s", unit="rows/s"):
    panels.append(dict(title=title, sub=sub, unit=unit, rows=[dict(label=r["label"], v=r[key], k=KIND(r["arm"]),
                                                                   t=r.get("median_s")) for r in rows]))
m = res["micro"]
add("HTTP · one page", "50k rows fetched + parsed outside Spark", m, 0)
for act, sub in (("scan", "1M rows, 4 partitions · read + sum in Spark"),
                 ("toPandas", "1M rows, 4 partitions · read + transfer to driver")):
    add(f"HTTP · {act}", sub, [r for r in res["spark"] if r["action"] == act], 0)
add("Gzip logs", "4 files × 250k lines · regex parse", res["logs"], 0)
add("NDJSON writer", "500k rows, 4 partitions · df.write", res["writers"], 0)
add("MQTT stream", "300 msgs × 1000 records · broker-bound", res["mqtt"], 0)
if "parquet" in res:
    add("Parquet · decode", "one file outside Spark, all row groups", res["parquet_micro"], 0)
    for q, sub in (("full scan (all columns)", "10M rows, 8 files written by Spark with defaults · 8 aggregates over all columns"),
                   ("one column", "sum(d) · built-in prunes automatically; data sources get .schema('d double')"),
                   ("count(*)", "built-in reads no columns; data sources read one (.schema('id long'))")):
        add("Parquet · " + q.split(" (")[0], sub, [r for r in res["parquet"] if r["query"] == q], 0)

if "fanout" in res:
    add("Fan-out · Spark", "400k rows, 4 partitions × 100 pages, 20 ms latency per request, 64 in flight",
        res["fanout"], 0)
    pr = res["fanout_probe"]
    for conc in (64, 512):
        add(f"Fan-out · {conc} in flight",
            f"one partition (100 pages) outside Spark, 20 ms latency; sequential arms use 1 request at a time. "
            f"Mock server tops out near {res.get('fanout_capacity_rps', 0):,.0f} req/s",
            [r for r in pr if r["latency_ms"] == 20 and r["concurrency"] in (1, conc)], 0,
            key="pages_per_s", unit="pages/s")
if "enrich" in res:
    add("Enrich · kernel", "H3 cell + xxh3 + word count, one 50k-row batch outside Spark", res["enrich_micro"], 0)
    add("Enrich · Spark", "2M generated rows, 4 partitions; the Python arms call C libraries too", res["enrich"], 0)

html = open("bench/plot_template.html").read().replace("/*DATA*/[]", json.dumps(panels))
open("docs/results.html", "w").write(html)
print("wrote docs/results.html")
