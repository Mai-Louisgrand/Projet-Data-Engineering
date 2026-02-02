'''
Kafka configuration for the OWID streaming pipeline
'''

import os
from typing import Dict, Union

# Detect run environment to set broker address (Docker vs local)
RUNNING_IN_DOCKER = os.getenv("RUNNING_IN_DOCKER", "0") == "1"

if RUNNING_IN_DOCKER:
    KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
else:
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
print(f"RUNNING_IN_DOCKER={RUNNING_IN_DOCKER}, KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}")

# Kafka topic for OWID vaccination events
OWID_TOPIC = "owid_vaccination_events"

# Producer configuration
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",              # fiability
    "linger.ms": 5,             # enable micro-batching for network efficiency
    "retries": 3,               # retry sending up to 3 times if failed
}

# Consumer configuration
CONSUMER_CONFIG: Dict[str, Union[str, int, float, bool, None]] = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "debug_consumer_group",
    "auto.offset.reset": "earliest", # lire depuis le début
}