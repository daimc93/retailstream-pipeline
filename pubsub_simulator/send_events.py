from google.cloud import pubsub_v1
import json
import time

# Leer configuración
with open("config/dev_config.json") as f:
    config = json.load(f)

project_id = config["project_id"]
topic_id = config["topic"]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Leer datos de prueba
with open("data/sample_transactions.json", "r") as f:
    records = json.load(f)

# Publicar cada mensaje como si fuera en tiempo real
for record in records:
    data = json.dumps(record).encode("utf-8")
    publisher.publish(topic_path, data=data)
    time.sleep(1)  # Simula flujo de streaming
