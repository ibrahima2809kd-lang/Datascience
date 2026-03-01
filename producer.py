import time
import random
import json
import os
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
import logging

# Configuration Kafka
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'blood_pressure'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8'),
    api_version=(0, 10, 1) # Ajoute cette ligne
)

fake = Faker('fr_FR')
NUM_PATIENTS = 10

#  fonction détection anomalies
def detect_anomaly(sys, dia):
    if sys >= 140 or dia >= 90:
        return "HYPERTENSION"
    elif sys <= 90 or dia <= 60:
        return "HYPOTENSION"
    return "NORMAL"

def create_blood_pressure(p_id, name, age, sys, dia):
    now = datetime.now(timezone.utc).isoformat()

    status = detect_anomaly(sys, dia)  #  ajout anomalie

    obs_data = {
        "metadata": {
            "timestamp": now,
            "resource_type": "Observation"
        },
        "resource": {
            "resourceType": "Observation",
            "status": "final",
            "subject": {
                "reference": f"Patient/{p_id}",
                "display": name   # ⭐ utile pour consumer
            },
            "effectiveDateTime": now,
            "component": [
                {"code": {"coding": [{"code": "8480-6"}]}, "valueQuantity": {"value": sys}},
                {"code": {"coding": [{"code": "8462-4"}]}, "valueQuantity": {"value": dia}}
            ]
        },
        "anomaly_status": status   # ⭐ champ clé pour consumer
    }

    return obs_data

# Initialisation patients
patients = []
for i in range(NUM_PATIENTS):
    patients.append({
        "id": f"PAT-2026-{i+1:03d}",
        "name": fake.name(),
        "age": random.randint(20, 85),
        "gender": random.choice(['M', 'F']),
        "sys": random.randint(110, 135),
        "dia": random.randint(70, 85)
    })

print(f"Producteur FHIR lancé. Envoi vers Kafka : {KAFKA_BOOTSTRAP}")

try:
    while True:
        date = datetime.now().strftime("%d/%m/%Y")
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"_Relevé le {date}_\n--- à  {timestamp} ---")

        for p in patients:
            p["sys"] += random.randint(-3, 3)
            p["dia"] += random.randint(-2, 2)

            message = create_blood_pressure(p["id"], p["name"], p["age"], p["sys"], p["dia"])

            producer.send(TOPIC_NAME, key=p["id"], value=message)

            print(f"Envoyé : [{p['id']}] {p['name']:35} | {p['age']} ans | {p['gender']:2} || {p['sys']}/{p['dia']} mmHg || {message['anomaly_status']}")

        producer.flush()
        time.sleep(3)

except KeyboardInterrupt:
    print("Arrêt.")