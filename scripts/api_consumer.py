from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'raw_api_events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for messages on 'raw_api_events'...")
for message in consumer:
    print(f"Received: {message.value}")