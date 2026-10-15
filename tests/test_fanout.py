import pytest

import mock_server
from spark_datasources import group


@pytest.fixture(scope="module")
def async_url():
    p, url = mock_server.start_async_process()
    yield url
    p.terminate()


@pytest.mark.parametrize("fmt", group("fanout_"))
def test_fanout_matches_http(spark, async_url, fmt):
    kw = dict(total_rows=5300, partitions=3, page_size=250, latency_ms=2, concurrency=16)
    got = spark.read.format(fmt).option("url", async_url)
    for k, v in kw.items():
        got = got.option(k, v)
    rows = sorted(got.load().collect(), key=lambda r: r.id)
    assert [r.id for r in rows] == list(range(5300))          # nothing lost, duplicated or short
    assert rows[4321].asDict() == {"id": 4321, "value": (4321 % 1000) / 8, "name": "user-4321",
                                   "ts": 1_700_004_321}
