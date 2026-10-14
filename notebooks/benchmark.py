import marimo

app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    from pathlib import Path

    import marimo as mo

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bench"))
    import harness
    import pandas as pd

    return harness, mo, pd


@app.cell
def _(mo):
    mo.md(
        """
        # PySpark HTTP data source: Python vs Rust vs Cython
        Five implementations of the same `DataSourceReader` against a local paginated JSON API.
        Pick parameters below, then press **Run**.
        """
    )
    return


@app.cell
def _(mo):
    rows = mo.ui.slider(100_000, 3_000_000, step=100_000, value=1_000_000, label="rows")
    parts = mo.ui.slider(1, 8, value=4, label="partitions")
    page = mo.ui.slider(5_000, 200_000, step=5_000, value=50_000, label="page size")
    repeats = mo.ui.slider(1, 7, value=3, label="repeats")
    run = mo.ui.run_button(label="Run benchmark")
    mo.vstack([rows, parts, page, repeats, run])
    return page, parts, repeats, rows, run


@app.cell
def _(harness):
    srv, url = harness.start_server()
    spark = harness.get_spark()
    return spark, url


@app.cell
def _(harness, mo, page, parts, pd, repeats, rows, run, spark, url):
    mo.stop(not run.value, mo.md("_Press Run._"))
    micro = pd.DataFrame(harness.micro(url, page.value, repeats.value + 2))
    e2e = pd.DataFrame(
        harness.spark_bench(spark, url, rows.value, parts.value, page.value, repeats=repeats.value)
    )
    return e2e, micro


@app.cell
def _(micro, mo):
    mo.vstack([mo.md("## Micro: one page, outside Spark"), micro])
    return


@app.cell
def _(e2e, mo):
    import altair as alt

    chart = alt.Chart(e2e).mark_bar().encode(
        x=alt.X("label:N", sort=None, title=None), y=alt.Y("rows_per_s:Q", title="rows / s"),
        color="label:N", column="action:N",
    )
    mo.vstack([mo.md("## End-to-end through Spark (higher is better)"), chart, e2e])
    return


if __name__ == "__main__":
    app.run()
