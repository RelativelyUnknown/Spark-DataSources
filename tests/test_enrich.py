import pytest

from spark_datasources import group
from spark_datasources.enrich import enrich_py_arrow, enrich_rust, make_points


def test_rust_equals_python_libs_exactly():
    """h3o vs h3-py (C), xxhash-rust vs xxhash (C), unicode-segmentation vs re: every column identical."""
    batch = make_points(0, 20000)
    for res in (5, 9, 12):
        a, b = enrich_py_arrow(batch, res), enrich_rust(batch, res)
        assert a.schema.equals(b.schema)
        assert a.equals(b), f"resolution {res}"


def test_known_values():
    row = enrich_rust(make_points(0, 1), 9).to_pylist()[0]
    # "event 0 at node- 0 café ok" -> event, 0, at, node, 0, café, ok
    assert row == {"id": 0, "lat": -60.0, "lng": -180.0, "h3": 621122241529380863,
                   "hash": -6812208678932119692, "words": 7}


@pytest.mark.parametrize("fmt", group("enrich_"))
def test_enrich_in_spark(spark, fmt):
    df = (spark.read.format(fmt).option("total_rows", 7000).option("partitions", 3)
          .option("batch_rows", 1000).option("resolution", 9).load())
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert [r.id for r in rows] == list(range(7000))
    assert rows[0].asDict() == {"id": 0, "lat": -60.0, "lng": -180.0, "h3": 621122241529380863,
                                "hash": -6812208678932119692, "words": 7}
    ref = sorted(spark.read.format("enrich_py").option("total_rows", 7000).option("partitions", 3)
                 .option("batch_rows", 1000).load().collect())
    assert sorted(rows) == ref
