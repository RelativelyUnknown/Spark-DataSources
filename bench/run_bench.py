"""CLI: python bench/run_bench.py [--rows 1000000] [--partitions 4] [--repeats 3] [--json out.json]"""
import argparse
import json

import harness


def table(rows, cols):
    head = "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
    f = lambda v: f"{v:,.3f}" if isinstance(v, float) and v < 1000 else (f"{v:,.0f}" if isinstance(v, float) else str(v))
    return head + "\n".join("| " + " | ".join(f(r[c]) for c in cols) + " |" for r in rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--page-size", type=int, default=50_000)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--json")
    a = ap.parse_args()
    srv, url = harness.start_server()
    print("== micro: one page fetch+parse outside Spark ==")
    m = harness.micro(url, a.page_size, a.repeats + 2)
    print(table(m, ["label", "rows", "median_s", "rows_per_s"]))
    spark = harness.get_spark()
    print(f"\n== Spark end-to-end: {a.rows:,} rows, {a.partitions} partitions ==")
    s = harness.spark_bench(spark, url, a.rows, a.partitions, a.page_size, repeats=a.repeats)
    print(table(s, ["label", "action", "median_s", "min_s", "rows_per_s"]))
    if a.json:
        json.dump({"micro": m, "spark": s}, open(a.json, "w"), indent=1)


if __name__ == "__main__":
    main()
