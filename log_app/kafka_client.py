from kafka import KafkaProducer
import json
import os

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

def publish_to_kafka(topic, message, unique_name):
    producer.send(topic, message, unique_name.encode('utf-8'))
    producer.flush()
