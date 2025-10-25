from kafka import KafkaProducer, KafkaAdminClient
import json
import os, uuid
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from django.utils import timezone

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

def delete_topic(topic_name):
    try:
        admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' deleted successfully.")
    except UnknownTopicOrPartitionError:
        print(f"Topic '{topic_name}' does not exist and cannot be deleted.")

def publish_to_kafka(topic, message, unique_name):
    producer.send(topic, message, unique_name.encode('utf-8'))
    producer.flush()
