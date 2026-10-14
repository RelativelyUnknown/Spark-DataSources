import time

import pytest

import mqtt_broker
from spark_datasources import group


@pytest.fixture(scope="module")
def port():
    return mqtt_broker.start(18831)


@pytest.mark.parametrize("fmt", group("mqtt_"))
def test_stream(spark, port, fmt):
    topic = f"t/{fmt}"
    q = (spark.readStream.format(fmt).option("port", port).option("topic", topic).load()
         .writeStream.format("memory").queryName(f"q_{fmt}").trigger(processingTime="200 milliseconds").start())
    try:
        time.sleep(1.5)  # let the driver-side subscription establish
        total = mqtt_broker.publish(port, topic, n_msgs=5, recs_per_msg=200)
        deadline = time.time() + 30
        n = 0
        while time.time() < deadline and n < total:
            n = spark.sql(f"select count(*) c from q_{fmt}").first().c
            time.sleep(0.3)
        assert n == total
        ids = {r.id for r in spark.sql(f"select id from q_{fmt}").collect()}
        assert ids == set(range(total))
    finally:
        q.stop()
