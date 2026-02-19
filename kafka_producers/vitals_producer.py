"""
kafka_producers/vitals_producer.py
====================================
Simulates ICU/ward monitors streaming patient vital signs into Kafka.

In a real hospital, this logic lives inside the medical device middleware
(e.g., Philips IntelliVue, GE Centricity) which pushes HL7 messages via
an integration engine. For our purposes, this Python script serves the
same role: it continuously publishes JSON messages to a Kafka topic.

Key concepts demonstrated:
  - Kafka Producer API and message serialization
  - Partitioning strategy (same patient always goes to same partition
    via key=patient_id — ensuring ordered processing per patient)
  - Realistic anomaly injection for testing alerting logic
  - Graceful shutdown handling

Run with:
  python kafka_producers/vitals_producer.py

Stop with Ctrl+C — it will flush and close cleanly.
"""

import json
import time
import random
import signal
import sys
from datetime import datetime

# ── Kafka import with helpful error ─────────────────────────────────────────
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("ERROR: kafka-python not installed.")
    print("Run: pip install kafka-python")
    sys.exit(1)

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKERS = "localhost:9092"
VITALS_TOPIC  = "patient-vitals"
SEND_INTERVAL_SECONDS = 5   # How often to send a batch
MONITORED_PATIENTS    = 30  # Number of patients with active monitors

random.seed(42)

# ── Vital Sign Ranges ─────────────────────────────────────────────────────────
# Each vital has: (normal_low, normal_high, critical_low, critical_high)
# We use these to generate both normal readings and realistic abnormals.
VITAL_RANGES = {
    "heart_rate":      (55,  100, 30, 180),
    "systolic_bp":     (100, 135, 70, 200),
    "diastolic_bp":    (65,  85,  40, 120),
    "spo2_pct":        (95,  100, 85, 100),   # SpO2 below 90 = critical
    "temperature_c":   (36.5, 37.5, 35.0, 40.5),
    "resp_rate":       (12,  20,  6,   35),    # breaths per minute
}

# Simulate a fixed pool of monitored patients
PATIENTS = [f"P{random.randint(10000, 99999)}" for _ in range(MONITORED_PATIENTS)]
DEPARTMENTS = ["ICU", "Cardiology", "Neurology", "Emergency"]
PATIENT_DEPARTMENTS = {p: random.choice(DEPARTMENTS) for p in PATIENTS}

# Track per-patient "health trajectory" so anomalies are realistic
# (a patient deteriorates gradually, not randomly per message)
PATIENT_STATES = {p: "stable" for p in PATIENTS}


def generate_vital_reading(patient_id: str, force_anomaly: bool = False) -> dict:
    """
    Generate a single vital sign reading for a patient.

    If the patient is in "deteriorating" state, their vitals trend toward
    the critical range — simulating a real clinical deterioration pattern
    rather than random noise.
    """
    state = PATIENT_STATES[patient_id]

    # ~3% chance of a patient starting to deteriorate each cycle
    if state == "stable" and random.random() < 0.03:
        PATIENT_STATES[patient_id] = "deteriorating"
        state = "deteriorating"
    # A deteriorating patient has 20% chance of recovering each cycle
    elif state == "deteriorating" and random.random() < 0.20:
        PATIENT_STATES[patient_id] = "stable"
        state = "stable"

    vitals = {}
    is_anomaly = False

    for vital, (n_low, n_high, c_low, c_high) in VITAL_RANGES.items():
        if state == "deteriorating" or force_anomaly:
            # Deteriorating: push toward critical range
            if vital in ("heart_rate", "systolic_bp", "temperature_c", "resp_rate"):
                value = random.uniform(n_high, c_high)   # trending high
            else:
                value = random.uniform(c_low, n_low)     # trending low (SpO2, diastolic)
            is_anomaly = True
        else:
            # Normal: stay within the normal range with small noise
            value = random.uniform(n_low, n_high)

        # Round to appropriate precision
        vitals[vital] = round(value, 1) if vital == "temperature_c" else int(value)

    return {
        "message_id":   f"VIT-{patient_id}-{int(time.time()*1000)}",
        "patient_id":   patient_id,
        "department":   PATIENT_DEPARTMENTS[patient_id],
        "timestamp":    datetime.utcnow().isoformat() + "Z",
        "patient_state": state,
        "is_anomaly":   is_anomaly,
        **vitals,
    }


def on_send_success(record_metadata):
    """Callback for successful Kafka sends — useful for debugging."""
    pass  # Silent in normal operation; uncomment below for verbose mode
    # print(f"  Sent to {record_metadata.topic}[{record_metadata.partition}] offset={record_metadata.offset}")


def on_send_error(exc):
    """Callback for failed Kafka sends — always log these."""
    print(f"  [ERROR] Failed to send message to Kafka: {exc}")


# ── Connect to Kafka ──────────────────────────────────────────────────────────
print(f"Connecting to Kafka at {KAFKA_BROKERS}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        # JSON serialization: dict → bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Key serialization: patient_id string → bytes
        # Using patient_id as the key means Kafka routes all messages for
        # the same patient to the SAME partition — guaranteeing ordered
        # processing per patient, which is critical for anomaly detection.
        key_serializer=lambda k: k.encode("utf-8"),
        # Batch settings for efficiency
        batch_size=16384,       # 16KB batches
        linger_ms=100,          # Wait up to 100ms to fill a batch
        compression_type="gzip", # Compress batches (saves ~60% bandwidth)
        # Reliability: wait for all replicas to acknowledge
        acks="all",
        retries=3,
    )
    print(f"Connected! Monitoring {MONITORED_PATIENTS} patients.")
    print("Press Ctrl+C to stop gracefully.\n")
except NoBrokersAvailable:
    print(f"\nERROR: Cannot connect to Kafka at {KAFKA_BROKERS}")
    print("Make sure Docker is running: docker compose up -d")
    sys.exit(1)


# ── Graceful Shutdown Handler ─────────────────────────────────────────────────
messages_sent = 0
running = True

def shutdown(signum, frame):
    global running
    print(f"\nShutdown signal received. Flushing {messages_sent} messages sent so far...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ── Main Send Loop ────────────────────────────────────────────────────────────
print(f"{'─'*60}")
print(f"{'Timestamp':<25} {'Patients':<10} {'Anomalies':<12} {'Total Sent'}")
print(f"{'─'*60}")

while running:
    batch_start = time.time()
    batch_anomalies = 0

    for patient_id in PATIENTS:
        if not running:
            break

        reading = generate_vital_reading(patient_id)
        if reading["is_anomaly"]:
            batch_anomalies += 1

        # Send with patient_id as partition key — ensures ordered processing
        producer.send(
            VITALS_TOPIC,
            key=patient_id,
            value=reading,
        ).add_callback(on_send_success).add_errback(on_send_error)

    # Flush ensures all buffered messages are actually sent before we sleep
    producer.flush()
    messages_sent += len(PATIENTS)

    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'):<25} "
          f"{len(PATIENTS):<10} {batch_anomalies:<12} {messages_sent:,}")

    # Sleep for the remainder of the interval
    elapsed = time.time() - batch_start
    sleep_time = max(0, SEND_INTERVAL_SECONDS - elapsed)
    time.sleep(sleep_time)

# ── Clean Shutdown ────────────────────────────────────────────────────────────
print(f"\nTotal messages sent: {messages_sent:,}")
producer.close()
print("Producer closed cleanly. Goodbye.")
