from kafka import KafkaProducer
import json
import os

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

def publish_to_kafka(topic, message):
    producer.send(topic, message)
    producer.flush()
