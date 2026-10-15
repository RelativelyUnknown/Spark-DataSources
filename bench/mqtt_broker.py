"""In-process amqtt broker + publisher helper for the MQTT workload."""
import asyncio
import threading
import time

import mock_server

CFG = {"listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:{port}"}}, "sys_interval": 0,
       "auth": {"allow-anonymous": True, "plugins": ["auth_anonymous"]}, "topic-check": {"enabled": False}}


def start(port=18830):
    import copy

    from amqtt.broker import Broker

    cfg = copy.deepcopy(CFG)
    cfg["listeners"]["default"]["bind"] = f"127.0.0.1:{port}"
    ready = threading.Event()

    def run():
        async def main():
            await Broker(cfg).start()
            ready.set()
            await asyncio.Event().wait()

        asyncio.run(main())

    threading.Thread(target=run, daemon=True).start()
    assert ready.wait(15), "broker did not start"
    time.sleep(0.3)
    return port


def publish(port, topic, n_msgs, recs_per_msg=1000):
    """Publish n_msgs JSON-array payloads (deterministic ids 0..n*recs). Returns total records."""
    import paho.mqtt.client as mqtt

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.connect("127.0.0.1", port)
    c.loop_start()
    infos = [c.publish(topic, mock_server.page_body(i * recs_per_msg, recs_per_msg), qos=1)
             for i in range(n_msgs)]
    for i in infos:
        i.wait_for_publish()
    c.loop_stop()
    c.disconnect()
    return n_msgs * recs_per_msg
