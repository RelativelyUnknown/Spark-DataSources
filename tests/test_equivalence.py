import pytest

from spark_datasources import SOURCES


def read(spark, fmt, url, **opts):
    r = spark.read.format(fmt).option("url", url)
    for k, v in opts.items():
        r = r.option(k, v)
    return r.load()


def test_all_arms_registered():
    assert set(SOURCES) == {"http_py", "http_py_arrow", "http_rust", "http_rust_arrow", "http_cython"}


@pytest.mark.parametrize("fmt", sorted(SOURCES))
def test_matches_expected(spark, server_url, fmt):
    df = read(spark, fmt, server_url, total_rows=2500, partitions=3, page_size=400)
    assert df.rdd.getNumPartitions() == 3
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert len(rows) == 2500
    assert rows[0].asDict() == {"id": 0, "value": 0.0, "name": "user-0", "ts": 1_700_000_000}
    assert rows[1234].asDict() == {
        "id": 1234, "value": (1234 % 1000) / 8, "name": "user-1234", "ts": 1_700_001_234}


def test_identical_across_arms(spark, server_url):
    ref = sorted(read(spark, "http_py", server_url, total_rows=1000).collect())
    for fmt in sorted(set(SOURCES) - {"http_py"}):
        assert sorted(read(spark, fmt, server_url, total_rows=1000).collect()) == ref, fmt
