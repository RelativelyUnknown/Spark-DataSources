"""Render bench/results.json into a self-contained animated HTML chart (docs/results.html)."""
import json
import sys

res = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "bench/results.json"))
KIND = lambda a: ("sp" if a == "parquet_builtin" else "pya" if a == "parquet_pyarrow" else
                  "rsa" if a == "parquet_rust" else
                  "py" if a in ("http_py", "logs_py", "ndjson_py", "mqtt_py") else
                  "pya" if a.endswith("py_arrow") else
                  "rs" if a.endswith(("_rust", "mqtt_rust")) else
                  "rsa" if a.endswith("rust_arrow") else "cy")
panels = []
def add(title, sub, rows, base):
    panels.append(dict(title=title, sub=sub, rows=[dict(label=r["label"], v=r["rows_per_s"], k=KIND(r["arm"]),
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

html = open("bench/plot_template.html").read().replace("/*DATA*/[]", json.dumps(panels))
open("docs/results.html", "w").write(html)
print("wrote docs/results.html")
