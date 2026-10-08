"""
ingestion/mqtt_to_kafka_bridge.py
---------------------------------
PPT layer 2 - INGESTION (MQTT Broker -> Apache Kafka).   *** OPTIONAL path ***

In the PPT, the patient's smartphone publishes readings to an MQTT broker
(MQTT is the light-weight protocol IoT devices use), and the readings are then
forwarded into Kafka. This small program is that "forwarder":

    MQTT topic  wearables/<patient_id>/vitals   --->   Kafka topic "vitals"

It subscribes to every patient's MQTT topic and re-publishes each message to
Kafka, keyed by patient_id (so each patient stays on one Kafka partition).

Run (inside Docker):  docker compose --profile mqtt up -d mosquitto mqtt-bridge generator-mqtt
"""

import os
import sys
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings  # noqa: E402

forwarded = 0


def connect_kafka():
    """Connect to Kafka, retrying while it starts up."""
    for attempt in range(1, 31):
        try:
            return KafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP, linger_ms=20, acks=1)
        except NoBrokersAvailable:
            print(f"[bridge] Kafka not ready (attempt {attempt}/30) - retrying in 3 s ...")
            time.sleep(3)
    sys.exit("[bridge] Could not connect to Kafka.")


producer = connect_kafka()


def on_connect(client, userdata, flags, rc):
    """Called when we connect to MQTT: subscribe to all patients' topics."""
    print(f"[bridge] connected to MQTT (code {rc}); subscribing to '{settings.MQTT_SUBSCRIBE}'")
    client.subscribe(settings.MQTT_SUBSCRIBE, qos=1)


def on_message(client, userdata, msg):
    """Called for every MQTT message: forward the raw JSON bytes to Kafka."""
    global forwarded
    patient_id = msg.topic.split("/")[1]          # wearables/<patient_id>/vitals
    producer.send(settings.KAFKA_TOPIC, key=patient_id.encode("utf-8"), value=msg.payload)
    forwarded += 1
    if forwarded % 500 == 0:
        print(f"[bridge] forwarded {forwarded} messages MQTT -> Kafka")


client = mqtt.Client(client_id="mqtt-kafka-bridge")
client.on_connect = on_connect
client.on_message = on_message

for attempt in range(1, 31):
    try:
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT, keepalive=60)
        break
    except OSError:
        print(f"[bridge] MQTT broker not ready (attempt {attempt}/30) - retrying in 3 s ...")
        time.sleep(3)
else:
    sys.exit("[bridge] Could not connect to the MQTT broker.")

print("[bridge] running. Press Ctrl+C to stop.")
try:
    client.loop_forever()      # blocks; handles reconnects automatically
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print(f"[bridge] stopped after forwarding {forwarded} messages.")
