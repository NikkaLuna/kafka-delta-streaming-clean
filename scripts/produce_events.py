from confluent_kafka import Producer
import json, time

import os

# Load the .env file in the current script directory
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_USERNAME"),
    'sasl.password': os.getenv("KAFKA_PASSWORD")
}


producer = Producer(conf)

for i in range(20):
    event = {
        "event_id": f"evt_{i}",
        "event_type": "click",
        "timestamp": "2025-04-04T15:00:00",
        "value": 50 + i
    }
    producer.produce("stream-input", value=json.dumps(event))
    producer.poll(0)
    print(f" Sent: {event}")
    time.sleep(0.5)

producer.flush()
