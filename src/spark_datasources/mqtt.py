"""MQTT streaming source (SimpleDataSourceStreamReader).

A paho client subscribes on the driver-side reader and buffers raw payloads; each micro-batch decodes
the new payloads (each a JSON array of records) with the pluggable `decode(list[bytes]) -> list[tuple]`.
Offsets are message counts. Replay after restart is not supported (MQTT has no log) - PoC only.
Options: host, port, topic.
"""
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader

from .base import SCHEMA


class MqttReader(SimpleDataSourceStreamReader):
    def __init__(self, options, decode):
        self.host = options.get("host", "127.0.0.1")
        self.port = int(options.get("port", 1883))
        self.topic = options["topic"]
        self.decode = decode
        self.buf = []
        self.client = None

    def _ensure(self):
        if self.client is not None:
            return
        import paho.mqtt.client as mqtt

        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        c.on_message = lambda _c, _u, m: self.buf.append(m.payload)
        c.connect(self.host, self.port)
        c.subscribe(self.topic, qos=1)
        c.loop_start()
        self.client = c

    def initialOffset(self):
        self._ensure()
        return {"n": 0}

    def read(self, start):
        self._ensure()
        end = len(self.buf)
        return iter(self.decode(self.buf[start["n"]:end])), {"n": end}

    def readBetweenOffsets(self, start, end):
        return iter(self.decode(self.buf[start["n"]:end["n"]]))

    def __getstate__(self):  # the live client / buffer stay on the driver
        d = dict(self.__dict__)
        d["client"], d["buf"] = None, []
        return d


def make_mqtt_source(name, decode):
    class _Source(DataSource):
        @classmethod
        def name(cls):
            return name

        def schema(self):
            return SCHEMA

        def simpleStreamReader(self, schema):
            return MqttReader(self.options, decode)

    _Source.__name__ = f"MqttSource_{name}"
    return _Source
