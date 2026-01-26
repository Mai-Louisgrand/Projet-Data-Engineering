# Configuration Kafka pour le pipeline streaming OWID

from typing import Dict, Union

# Adresse du broker
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

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
    "group.id": "test_consumer_group",
    "auto.offset.reset": "earliest", # lire depuis le début
}