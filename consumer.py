import json
import os
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'blood_pressure'

print(f" Analyseur d'alertes démarré sur {KAFKA_BOOTSTRAP}...")

consumer = KafkaConsumer(
    'blood_pressure',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Lit depuis le début si c'est la première fois
    enable_auto_commit=True,
    group_id='my-group-test',      # Change ce nom pour "réinitialiser" la lecture
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    api_version=(0, 10, 2)
)
print("En attente de messages...")
for message in consumer:
    data = message.value
    res = data.get('resource', {})
    p_id = res.get('subject', {}).get('reference', 'Inconnu')
    
    # Extraction des valeurs numériques
    components = res.get('component', [])
    name = res.get('subject', {}).get('display', 'Anonyme')
    if len(components) >= 2:
        sys = components[0]['valueQuantity']['value']
        dia = components[1]['valueQuantity']['value']
        
        # Logique d'alerte
        status = "NORMAL"
        if sys >= 140 or dia >= 90: status = " HYPERTENSION"
        elif sys <= 90 or dia <= 60: status = " HYPOTENSION"
        print(f"--- Donnée brute reçue pour Patient ID: {p_id}")
        p = message.value
        print(f"--- Donnée brute reçue : {p}")
        print(f"{name} || {sys}/{dia} mmHg || Status: {status}")

   