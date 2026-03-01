import json
import os
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

# ----------------------- Configuration Elasticsearch -----------------------
# Configuration simplifiée pour Elasticsearch 8.x
# On utilise 127.0.0.1 au lieu de localhost pour éviter les conflits IPv6 sur Windows
es = Elasticsearch(
    "http://127.0.0.1:9200",
    verify_certs=False,
    request_timeout=30
)

# Test de connexion
try:
    if es.ping():
        print("Connexion réussie à Elasticsearch !")
    else:
        print("Le serveur est actif mais ne répond pas au ping.")
except Exception as e:
    print(f" Erreur de connexion : {e}")

# ----------------------- Configuration Kafka -----------------------
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'blood_pressure'

print(f"Analyseur d'alertes démarré sur {KAFKA_BOOTSTRAP}...")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    auto_offset_reset='earliest',
    group_id='groupe_analyse_medicale',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ----------------------- Création dossier stockage normal -----------------------
os.makedirs("normal_data", exist_ok=True)

# ----------------------- Boucle de traitement Kafka -----------------------
for message in consumer:
    data = message.value
    res = data.get('resource', {})
    p_id = res.get('subject', {}).get('reference', 'Inconnu')
    name = res.get('subject', {}).get('display', 'Anonyme')

    # Extraction des composants systolique / diastolique
    components = res.get('component', [])
    
    if len(components) >= 2:
        sys = components[0]['valueQuantity']['value']
        dia = components[1]['valueQuantity']['value']

        # Détermination du statut médical
        status = "NORMAL"
        if sys >= 140 or dia >= 90:
            status = "HYPERTENSION"
        elif sys <= 90 or dia <= 60:
            status = "HYPOTENSION"

        print(f"\n--- Donnée brute reçue pour Patient ID: {p_id}")
        print(f"Systolique: {sys} | Diastolique: {dia}")
        print(f"Status médical: {status}")

        # Stockage local si NORMAL
        if status == "NORMAL":
            file_name = f"normal_data/{p_id.replace('/', '_')}.json"
            with open(file_name, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Donnée normale stockée: {file_name}")

        # Stockage dans Elasticsearch si anomalie
        else:
            try:
                es.index(index="blood_pressure_anomalies", document=data)
                print(f"Anomalie envoyée à Elasticsearch: {status}")
            except Exception as e:
                print(f"Erreur indexation Elasticsearch: {e}")