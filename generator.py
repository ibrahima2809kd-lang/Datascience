

from faker import Faker
import random
import json
from datetime import datetime

fake = Faker()

def generate_bp_observation():
    patient_id = fake.uuid4()

    systolic = random.randint(80, 180)
    diastolic = random.randint(50, 110)

    observation = {
        "resourceType": "Observation",
        "id": fake.uuid4(),
        "subject": {
            "reference": f"Patient/{patient_id}"
        },
        "effectiveDateTime": datetime.now().isoformat(),
        "component": [
            {
                "code": {"text": "Systolic Blood Pressure"},
                "valueQuantity": {"value": systolic, "unit": "mmHg"}
            },
            {
                "code": {"text": "Diastolic Blood Pressure"},
                "valueQuantity": {"value": diastolic, "unit": "mmHg"}
            }
        ]
    }

    return observation


if __name__ == "__main__":
    for _ in range(5):
        obs = generate_bp_observation()
        print(json.dumps(obs, indent=2))