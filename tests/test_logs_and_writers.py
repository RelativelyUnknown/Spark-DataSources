import json
import glob

import pytest

from spark_datasources import group

import gen_logs


@pytest.fixture(scope="module")
def log_dir(tmp_path_factory):
    return gen_logs.generate(str(tmp_path_factory.mktemp("logs")), files=3, lines_per_file=2000)


@pytest.mark.parametrize("fmt", group("logs_"))
def test_logs_expected(spark, log_dir, fmt):
    df = spark.read.format(fmt).option("path", log_dir).load()
    assert df.rdd.getNumPartitions() == 3
    rows = sorted(df.collect(), key=lambda r: r.ts)
    assert len(rows) == 6000
    assert rows[5].asDict() == {"ip": "10.5.5.5", "ts": 1_700_000_005, "method": "POST",
                                "path": "/api/v1/item/5?q=5", "status": 404, "bytes": 185}


def test_logs_identical_across_arms(spark, log_dir):
    ref = sorted(spark.read.format("logs_py").option("path", log_dir).load().collect())
    for fmt in group("logs_")[1:]:
        assert sorted(spark.read.format(fmt).option("path", log_dir).load().collect()) == ref, fmt


def _read_ndjson(d):
    out = []
    for f in glob.glob(f"{d}/part-*.ndjson"):
        out += [json.loads(l) for l in open(f)]
    return sorted(out, key=lambda r: r["id"])


@pytest.mark.parametrize("fmt", group("ndjson_"))
def test_writers(spark, server_url, tmp_path, fmt):
    df = (spark.read.format("http_py").option("url", server_url).option("total_rows", 3000)
          .option("partitions", 3).load())
    out = str(tmp_path / "out")
    df.write.format(fmt).option("path", out).mode("append").save()
    assert (tmp_path / "out" / "_SUCCESS").read_text() == "3000"
    got = _read_ndjson(out)
    assert got == sorted((r.asDict() for r in df.collect()), key=lambda r: r["id"])
