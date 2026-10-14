import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bench"))
import mock_server  # noqa: E402

from spark_datasources import register_all  # noqa: E402


@pytest.fixture(scope="session")
def server_url():
    srv, url = mock_server.start()
    yield url
    srv.shutdown()


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    s = SparkSession.builder.master("local[2]").appName("tests").config(
        "spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    register_all(s)
    yield s
    s.stop()
