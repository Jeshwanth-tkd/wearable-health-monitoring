"""
generator/wearable_simulator.py
-------------------------------
PPT layer 1 - WEARABLE DEVICES (+ the smartphone BLE gateway).

Pretends to be the wearables of ~20 patients (smartwatch / fitness band /
ECG patch / BP monitor). Every 1-2 seconds each patient's device sends one
reading:

    patient_id, device_id, timestamp, heart_rate, spo2, systolic_bp,
    diastolic_bp, body_temp, steps, fall_detected

* Most readings are NORMAL for that patient.
* Some patients are "Medium" or "High" risk (borderline baseline vitals and
  more frequent problems) so the MLlib model has something to find.
* Short ABNORMAL EPISODES happen at random so the alert rules fire:
      HR > 120, SpO2 < 92, BP > 140/90, temperature > 38, fall detected.
* A few INVALID readings (missing values, impossible numbers) are sent on
  purpose, so you can show that Spark's noise filter drops them.

Where the readings go (--mode):
    kafka  (default) publish JSON straight to the Kafka topic "vitals"
    mqtt             publish to the MQTT broker, like the PPT diagram
                     (the MQTT -> Kafka bridge then forwards them)
    print            just print to the screen (test without Kafka)

Run inside Docker:   docker compose up -d generator
Run a quick test:    python generator/wearable_simulator.py --mode print --duration 5
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

# Make "common" importable when this file is run directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings  # noqa: E402

# --------------------------------------------------------------------------
# Patient health profiles (hidden from the pipeline – the pipeline only sees
# the readings and must work out the risk by itself).
#   baseline = the patient's normal average value for each vital sign
#   spike_rate = chance (per reading) that an abnormal episode starts
# --------------------------------------------------------------------------
PROFILES = {
    "low":    {"hr": 72,  "spo2": 97.6, "sys": 116, "dia": 76, "temp": 36.7, "spike_rate": 0.002},
    "medium": {"hr": 90,  "spo2": 94.4, "sys": 131, "dia": 82, "temp": 37.0, "spike_rate": 0.006},
    "high":   {"hr": 100, "spo2": 93.4, "sys": 134, "dia": 84, "temp": 37.5, "spike_rate": 0.015},
}

# Random "noise" (standard deviation) added to every reading.
NOISE = {"hr": 3.0, "spo2": 0.5, "sys": 2.5, "dia": 2.0, "temp": 0.1}

# Types of abnormal episode and how often each one is chosen.
EPISODE_TYPES = ["tachycardia", "hypoxia", "hypertension", "fever", "fall"]
EPISODE_WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08]

# Chance that a reading is deliberately broken (sensor error).
INVALID_RATE = 0.004

WARDS = ["Cardiology", "General Medicine", "Geriatrics", "Pulmonology"]
DEVICES = ["Smartwatch", "Fitness Band", "ECG Patch", "BP Monitor"]


class Patient:
    """One patient wearing one device. Keeps its own state between readings."""

    def __init__(self, number, profile, rng):
        self.rng = rng
        self.patient_id = f"P-{number:03d}"
        self.profile_name = profile
        self.base = PROFILES[profile]
        self.device_type = rng.choice(DEVICES)
        self.device_id = f"DEV-{number:03d}"
        self.ward = rng.choice(WARDS)
        self.gender = rng.choice(["F", "M"])
        age_range = {"low": (22, 60), "medium": (45, 75), "high": (60, 85)}[profile]
        self.age = rng.randint(*age_range)
        self.steps_today = rng.randint(500, 4000)    # cumulative step counter
        self.steps_day = datetime.now(timezone.utc).date()
        self.episode = None                          # current abnormal episode type
        self.episode_left = 0                        # readings left in that episode
        self.next_send = time.time() + rng.uniform(0, 2)

    # ------------------------------------------------------------------
    def _normal(self, key):
        """Baseline value + Gaussian noise."""
        return self.rng.gauss(self.base[key], NOISE[key])

    def make_reading(self):
        """Build one reading (a Python dict). Returns (reading, started_episode)."""
        rng = self.rng
        hr = self._normal("hr")
        spo2 = min(100.0, self._normal("spo2"))
        sys_bp = self._normal("sys")
        dia_bp = self._normal("dia")
        temp = self._normal("temp")
        fall = False
        started = None

        # Maybe start a new abnormal episode (lasts 3-8 readings).
        if self.episode is None and rng.random() < self.base["spike_rate"]:
            self.episode = rng.choices(EPISODE_TYPES, EPISODE_WEIGHTS)[0]
            self.episode_left = 1 if self.episode == "fall" else rng.randint(3, 8)
            started = self.episode

        # Apply the episode to this reading.
        if self.episode == "tachycardia":
            hr = rng.uniform(125, 155)
        elif self.episode == "hypoxia":
            spo2 = rng.uniform(85, 91.5)
            hr += 10
        elif self.episode == "hypertension":
            sys_bp = rng.uniform(145, 185)
            dia_bp = rng.uniform(92, 110)
        elif self.episode == "fever":
            temp = rng.uniform(38.2, 39.8)
            hr += 15
        elif self.episode == "fall":
            fall = True
            hr += 20
        if self.episode is not None:
            self.episode_left -= 1
            if self.episode_left <= 0:
                self.episode = None

        # Step counter (resets at midnight UTC). No steps while falling.
        today = datetime.now(timezone.utc).date()
        if today != self.steps_day:
            self.steps_day, self.steps_today = today, 0
        if not fall:
            self.steps_today += rng.choice([0, 0, 1, 2, 3, 4])

        reading = {
            "patient_id": self.patient_id,
            "device_id": self.device_id,
            # UTC time, e.g. "2026-10-08 04:15:02.123"
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "heart_rate": round(hr),
            "spo2": round(spo2, 1),
            "systolic_bp": round(sys_bp),
            "diastolic_bp": round(dia_bp),
            "body_temp": round(temp, 1),
            "steps": self.steps_today,
            "fall_detected": fall,
        }

        # Occasionally break the reading on purpose (sensor glitch).
        if rng.random() < INVALID_RATE:
            reading = self._break(reading)
        return reading, started

    def _break(self, reading):
        """Make the reading invalid in one of several realistic ways."""
        kind = self.rng.choice(["missing_hr", "spo2_over_100", "negative_hr", "missing_temp", "bp_swapped"])
        if kind == "missing_hr":
            reading["heart_rate"] = None
        elif kind == "spo2_over_100":
            reading["spo2"] = 105.0
        elif kind == "negative_hr":
            reading["heart_rate"] = -1
        elif kind == "missing_temp":
            del reading["body_temp"]
        elif kind == "bp_swapped":
            reading["systolic_bp"], reading["diastolic_bp"] = 60, 80
        reading["_invalid_reason"] = kind   # only for our own log; Spark ignores unknown fields
        return reading


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def build_patients(n, seed):
    """Create n patients: about 60% Low, 25% Medium, 15% High risk profiles."""
    rng = random.Random(seed)
    n_high = max(1, round(n * 0.15))
    n_medium = max(1, round(n * 0.25))
    n_low = max(0, n - n_high - n_medium)
    profiles = ["low"] * n_low + ["medium"] * n_medium + ["high"] * n_high
    rng.shuffle(profiles)
    return [Patient(i + 1, profiles[i], random.Random(seed * 1000 + i)) for i in range(n)]


def write_patients_csv(patients):
    """Write the patient master list (the Hive 'patients' table reads this)."""
    os.makedirs(settings.PATIENTS_DIR, exist_ok=True)
    with open(settings.PATIENTS_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["patient_id", "age", "gender", "ward", "device_type", "device_id"])
        for p in patients:
            writer.writerow([p.patient_id, p.age, p.gender, p.ward, p.device_type, p.device_id])
    print(f"[simulator] wrote patient list -> {settings.PATIENTS_CSV}")


def make_kafka_sender(bootstrap):
    """Connect to Kafka (retrying while Kafka is still starting) and return a send function."""
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                key_serializer=lambda k: k.encode("utf-8"),                 # key = patient_id
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),   # value = JSON reading
                linger_ms=20,
                acks=1,
            )
            print(f"[simulator] connected to Kafka at {bootstrap}, topic '{settings.KAFKA_TOPIC}'")
            break
        except NoBrokersAvailable:
            print(f"[simulator] Kafka not ready yet (attempt {attempt}/30) - retrying in 3 s ...")
            time.sleep(3)
    else:
        sys.exit("[simulator] Could not connect to Kafka. Is it running?  docker compose ps")

    def send(reading):
        # Same patient -> same key -> same Kafka partition (keeps each patient's readings in order).
        producer.send(settings.KAFKA_TOPIC, key=reading["patient_id"], value=reading)

    return send, producer.flush


def make_mqtt_sender(host, port):
    """Connect to the MQTT broker and return a send function."""
    import paho.mqtt.client as mqtt

    client = mqtt.Client(client_id="wearable-gateway-simulator")
    for attempt in range(1, 31):
        try:
            client.connect(host, port, keepalive=60)
            break
        except OSError:
            print(f"[simulator] MQTT broker not ready (attempt {attempt}/30) - retrying in 3 s ...")
            time.sleep(3)
    else:
        sys.exit("[simulator] Could not connect to the MQTT broker.")
    client.loop_start()
    print(f"[simulator] connected to MQTT at {host}:{port}")

    def send(reading):
        topic = settings.MQTT_TOPIC_PATTERN.format(patient_id=reading["patient_id"])
        client.publish(topic, json.dumps(reading), qos=1)

    return send, lambda: None


def make_print_sender():
    """Dry-run: print each reading instead of sending it."""
    def send(reading):
        print(json.dumps(reading))
    return send, lambda: None


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Wearable device data simulator")
    parser.add_argument("--patients", type=int, default=20, help="number of patients (default 20)")
    parser.add_argument("--mode", choices=["kafka", "mqtt", "print"], default="kafka")
    parser.add_argument("--bootstrap", default=settings.KAFKA_BOOTSTRAP, help="Kafka address")
    parser.add_argument("--mqtt-host", default=settings.MQTT_HOST)
    parser.add_argument("--mqtt-port", type=int, default=settings.MQTT_PORT)
    parser.add_argument("--duration", type=float, default=0, help="stop after N seconds (0 = run forever)")
    parser.add_argument("--seed", type=int, default=42, help="random seed (same seed = same patients)")
    args = parser.parse_args()

    patients = build_patients(args.patients, args.seed)
    if args.mode != "print":
        write_patients_csv(patients)

    if args.mode == "kafka":
        send, flush = make_kafka_sender(args.bootstrap)
    elif args.mode == "mqtt":
        send, flush = make_mqtt_sender(args.mqtt_host, args.mqtt_port)
    else:
        send, flush = make_print_sender()

    print(f"[simulator] streaming {len(patients)} patients every 1-2 s. Press Ctrl+C to stop.")
    start = last_report = time.time()
    sent = invalid = episodes = 0

    try:
        while True:
            now = time.time()
            for p in patients:
                if now >= p.next_send:
                    reading, started = p.make_reading()
                    send(reading)
                    sent += 1
                    if "_invalid_reason" in reading:
                        invalid += 1
                    if started:
                        episodes += 1
                        if args.mode != "print":
                            print(f"[simulator] !! {p.patient_id}: abnormal episode started -> {started}")
                    p.next_send = now + p.rng.uniform(1.0, 2.0)   # next reading in 1-2 s

            if args.mode != "print" and now - last_report >= 10:
                print(f"[simulator] sent {sent} readings so far ({episodes} abnormal episodes, {invalid} invalid)")
                last_report = now
            if args.duration and now - start >= args.duration:
                break
            time.sleep(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        flush()
        print(f"[simulator] stopped. Total sent: {sent} ({episodes} abnormal episodes, {invalid} invalid)")


if __name__ == "__main__":
    main()
