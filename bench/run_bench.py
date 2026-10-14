"""CLI: python bench/run_bench.py [--rows 1000000] [--partitions 4] [--repeats 3] [--json out.json]"""
import argparse
import json
import os

import harness


def table(rows, cols):
    head = "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
    f = lambda v: f"{v:,.3f}" if isinstance(v, float) and v < 1000 else (f"{v:,.0f}" if isinstance(v, float) else str(v))
    return head + "\n".join("| " + " | ".join(f(r[c]) for c in cols) + " |" for r in rows)


SECTIONS = ("http", "logs", "writers", "mqtt", "parquet", "fanout")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--page-size", type=int, default=50_000)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--only", nargs="+", choices=SECTIONS, default=list(SECTIONS),
                    help="run just these sections; results are merged into --json if it exists")
    ap.add_argument("--json")
    a = ap.parse_args()
    out = {}
    if a.json and os.path.exists(a.json):
        out = json.load(open(a.json))
    srv, url = harness.start_server()
    spark = harness.get_spark()
    cols = ["label", "rows", "median_s", "min_s", "rows_per_s"]
    if "http" in a.only:
        print("== micro: one page fetch+parse outside Spark ==")
        out["micro"] = harness.micro(url, a.page_size, a.repeats + 2)
        print(table(out["micro"], ["label", "rows", "median_s", "rows_per_s"]))
        print(f"\n== Spark end-to-end: {a.rows:,} rows, {a.partitions} partitions ==")
        out["spark"] = harness.spark_bench(spark, url, a.rows, a.partitions, a.page_size, repeats=a.repeats)
        print(table(out["spark"], ["label", "action", "median_s", "min_s", "rows_per_s"]))
    if "logs" in a.only:
        print("\n== gzip access logs: 4 files x 250k lines ==")
        out["logs"] = harness.logs_bench(spark, harness.make_logs(4, 250_000), a.repeats)
        print(table(out["logs"], cols))
    if "writers" in a.only:
        print("\n== NDJSON writer: 500k rows, 4 partitions ==")
        out["writers"] = harness.writer_bench(spark, url, 500_000, 4, a.repeats)
        print(table(out["writers"], cols))
    if "mqtt" in a.only:
        print("\n== MQTT stream: 300 msgs x 1000 records (publish -> all rows in sink) ==")
        out["mqtt"] = harness.mqtt_bench(spark)
        print(table(out["mqtt"], cols))
    if "parquet" in a.only:
        pq_dir = harness.make_parquet(spark)
        print("\n== Parquet decode, one file outside Spark ==")
        out["parquet_micro"] = harness.parquet_micro(pq_dir)
        print(table(out["parquet_micro"], ["label", "rows", "median_s", "rows_per_s"]))
        print("\n== Parquet in Spark: 10M rows, 8 files (written by Spark with defaults) ==")
        out["parquet"] = harness.parquet_bench(spark, pq_dir, a.repeats)
        print(table(out["parquet"], ["label", "query", "partitions", "median_s", "min_s", "rows_per_s"]))
    if "fanout" in a.only:
        aproc, aurl = harness.mock_server.start_async_process()
        try:
            out["fanout_capacity_rps"] = harness.server_capacity(aurl)
            print(f"\n== fan-out: async mock server capacity ~{out['fanout_capacity_rps']:,.0f} req/s ==")
            print("== fan-out client probe: 100 pages of 1000 rows, outside Spark ==")
            out["fanout_probe"] = harness.fanout_probe(aurl)
            print(table(out["fanout_probe"], ["label", "latency_ms", "concurrency", "median_s", "pages_per_s"]))
            print("\n== fan-out in Spark: 400k rows, 4 partitions x 100 pages, 20 ms latency, concurrency 64 ==")
            out["fanout"] = harness.fanout_bench(spark, aurl)
            print(table(out["fanout"], ["label", "rows", "median_s", "min_s", "rows_per_s"]))
        finally:
            aproc.terminate()
    if a.json:
        json.dump(out, open(a.json, "w"), indent=1)


if __name__ == "__main__":
    main()
