"""
tests/test_logic.py
-------------------
Quick checks of the project's logic that need NO Kafka, Spark or Docker:
  1. the simulator produces mostly-normal readings with some alerts
  2. invalid readings are generated (so the noise filter has work to do)
  3. the risk score gives Low / Medium / High for the three patient profiles

Run:   python -m unittest tests/test_logic.py -v
(or inside Docker: docker compose run --rm generator python3 -m unittest tests/test_logic.py -v)
"""

import os
import statistics
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings  # noqa: E402
from generator.wearable_simulator import build_patients  # noqa: E402


def is_valid(r):
    """Same rules as the Spark noise filter (spark/streaming_job.py)."""
    for field, (lo, hi) in settings.VALID_RANGES.items():
        v = r.get(field)
        if v is None or not (lo <= v <= hi):
            return False
    return r["systolic_bp"] > r["diastolic_bp"] and r.get("steps", -1) >= 0


def alert_types(r):
    """Same threshold rules as the Spark alert stream."""
    rules = settings.ALERT_RULES
    out = []
    if r["heart_rate"] > rules["HIGH_HR"]["limit"]:
        out.append("HIGH_HR")
    if r["spo2"] < rules["LOW_SPO2"]["limit"]:
        out.append("LOW_SPO2")
    if r["systolic_bp"] > rules["HIGH_BP"]["sys_limit"] or r["diastolic_bp"] > rules["HIGH_BP"]["dia_limit"]:
        out.append("HIGH_BP")
    if r["body_temp"] > rules["FEVER"]["limit"]:
        out.append("FEVER")
    if r["fall_detected"]:
        out.append("FALL")
    return out


class SimulatorAndRiskTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Simulate 30 "minutes": 40 readings per patient per minute (~1 every 1.5 s).
        cls.patients = build_patients(20, seed=42)
        cls.windows = []      # (profile, risk_label) for each patient-minute
        cls.readings = []
        for p in cls.patients:
            for _minute in range(30):
                batch = [p.make_reading()[0] for _ in range(40)]
                cls.readings.extend(batch)
                good = [r for r in batch if is_valid(r)]
                pts = settings.risk_points(
                    avg_hr=statistics.mean(r["heart_rate"] for r in good),
                    avg_spo2=statistics.mean(r["spo2"] for r in good),
                    avg_sys=statistics.mean(r["systolic_bp"] for r in good),
                    avg_dia=statistics.mean(r["diastolic_bp"] for r in good),
                    avg_temp=statistics.mean(r["body_temp"] for r in good),
                    max_hr=max(r["heart_rate"] for r in good),
                    min_spo2=min(r["spo2"] for r in good),
                    falls=sum(1 for r in good if r["fall_detected"]),
                )
                cls.windows.append((p.profile_name, settings.risk_label(pts)))

    def test_patient_mix(self):
        names = [p.profile_name for p in self.patients]
        self.assertEqual((names.count("low"), names.count("medium"), names.count("high")), (12, 5, 3))

    def test_reading_fields(self):
        r = next(x for x in self.readings if is_valid(x))
        for field in ["patient_id", "timestamp", "heart_rate", "spo2", "systolic_bp",
                      "diastolic_bp", "body_temp", "steps", "fall_detected"]:
            self.assertIn(field, r)

    def test_some_invalid_readings(self):
        invalid = sum(1 for r in self.readings if not is_valid(r))
        share = invalid / len(self.readings)
        print(f"\n  invalid readings: {invalid} ({share:.2%})")
        self.assertGreater(invalid, 0)
        self.assertLess(share, 0.02)

    def test_alerts_fire_but_are_rare(self):
        valid = [r for r in self.readings if is_valid(r)]
        counts = {}
        for r in valid:
            for a in alert_types(r):
                counts[a] = counts.get(a, 0) + 1
        share = sum(1 for r in valid if alert_types(r)) / len(valid)
        print(f"\n  alert counts: {counts}  | readings with an alert: {share:.2%}")
        for a in ["HIGH_HR", "LOW_SPO2", "HIGH_BP", "FEVER", "FALL"]:
            self.assertIn(a, counts, f"no {a} alert generated")
        self.assertLess(share, 0.08, "too many alerts - readings should be mostly normal")

    def test_risk_labels_match_profiles(self):
        for profile, expected in [("low", 0), ("medium", 1), ("high", 2)]:
            labels = [lab for prof, lab in self.windows if prof == profile]
            agree = labels.count(expected) / len(labels)
            print(f"\n  {profile:6s} patients -> label {settings.RISK_LEVELS[expected]} in {agree:.0%} of minutes")
            self.assertGreater(agree, 0.75)


if __name__ == "__main__":
    unittest.main(verbosity=2)
