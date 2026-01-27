# Configuration Kafka pour le pipeline streaming OWID

import os
from typing import Dict, Union

# Adresse du broker selon si script run dans Docker ou en local
RUNNING_IN_DOCKER = os.getenv("RUNNING_IN_DOCKER", "0") == "1"

if RUNNING_IN_DOCKER:
    KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
else:
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
print(f"RUNNING_IN_DOCKER={RUNNING_IN_DOCKER}, KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}")

# Topic
OWID_TOPIC = "owid_vaccination_events"

# Paramètres du producer
PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",              # fiabilité 
    "linger.ms": 5,             # micro-batching pour performance réseau
    "retries": 3,               # fiabilité -> 3 essais si échec envoi
}

# Paramètres du consumer
CONSUMER_CONFIG: Dict[str, Union[str, int, float, bool, None]] = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "debug_consumer_group",
    "auto.offset.reset": "earliest", # lire depuis le début
}