import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker


# Load .env from project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

fake = Faker()

TOPIC = os.getenv("EH_TOPIC", "fraud-transactions")
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.2"))

running = True


def shutdown_handler(sig, frame):
    global running
    print("\nShutdown requested. Flushing producer...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered event to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def build_producer() -> Producer:
    required_env_vars = [
        "EH_BOOTSTRAP_SERVERS",
        "EH_CONNECTION_STRING",
        "EH_TOPIC",
    ]

    missing = [key for key in required_env_vars if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")

    return Producer(
        {
            "bootstrap.servers": os.environ["EH_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": os.environ["EH_CONNECTION_STRING"],
            "client.id": os.getenv("PRODUCER_CLIENT_ID", "fraud360-producer"),
            "acks": "all",
            "enable.idempotence": False,
            "message.timeout.ms": 120000,
            "request.timeout.ms": 60000,
            "retries": 5,
            "retry.backoff.ms": 1000,
        }
    )


def weighted_choice(options):
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


def generate_transaction_event() -> dict:
    country = weighted_choice(
        [
            ("US", 45),
            ("GB", 12),
            ("SE", 8),
            ("DE", 8),
            ("IN", 15),
            ("NG", 4),
            ("BR", 4),
            ("AE", 4),
        ]
    )

    channel = weighted_choice(
        [
            ("pos", 45),
            ("ecommerce", 40),
            ("atm", 15),
        ]
    )

    merchant_category = weighted_choice(
        [
            ("grocery", 22),
            ("fuel", 14),
            ("restaurant", 16),
            ("travel", 8),
            ("electronics", 9),
            ("gaming", 7),
            ("cash_advance", 4),
            ("luxury", 5),
            ("subscription", 15),
        ]
    )

    base_amount = round(random.expovariate(1 / 80), 2)

    # Inject rare high-value suspicious behavior
    if random.random() < 0.03:
        amount = round(random.uniform(800, 5000), 2)
    else:
        amount = base_amount

    event_ts = datetime.now(timezone.utc)

    risk_signals = {
        "high_amount": amount >= 750,
        "risky_country": country in {"NG", "BR", "AE"},
        "risky_category": merchant_category in {"cash_advance", "gaming", "luxury"},
        "card_not_present": channel == "ecommerce",
        "night_txn_utc": event_ts.hour in {0, 1, 2, 3, 4},
    }

    risk_score_hint = sum(1 for value in risk_signals.values() if value)

    return {
        "event_id": str(uuid.uuid4()),
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(10000, 99999),
        "card_id": random.randint(100000, 999999),
        "merchant_id": random.randint(1000, 9999),
        "merchant_name": fake.company(),
        "merchant_category": merchant_category,
        "amount": amount,
        "currency": "USD",
        "country": country,
        "city": fake.city(),
        "channel": channel,
        "device_id": str(uuid.uuid4()) if channel == "ecommerce" else None,
        "ip_address": fake.ipv4_public() if channel == "ecommerce" else None,
        "event_ts": event_ts.isoformat(),
        "risk_signals": risk_signals,
        "risk_score_hint": risk_score_hint,
        "is_suspicious_hint": risk_score_hint >= 2,
        "producer_app": "fraud360-python-producer",
        "schema_version": "1.0",
    }


def main():
    producer = build_producer()

    print(f"Producing fraud events to topic: {TOPIC}")

    while running:
        event = generate_transaction_event()
        key = event["transaction_id"]

        producer.produce(
            TOPIC,
            key=key,
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)
        print(json.dumps(event, indent=2))
        time.sleep(SLEEP_SECONDS)

    producer.flush(30)
    print("Producer stopped cleanly.")


if __name__ == "__main__":
    main()