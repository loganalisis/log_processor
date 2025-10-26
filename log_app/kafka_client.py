import logging
from kafka import KafkaProducer, KafkaAdminClient
import json
import os, uuid
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from django.utils import timezone

# admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
# admin_client = KafkaAdminClient(bootstrap_servers=f"kafka-28c7f468-shubham-20a9.j.aivencloud.com:26262")

# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],
#     value_serializer=lambda m: json.dumps(m).encode('utf-8')
# )
base_dir = os.path.dirname(__file__)

producer = KafkaProducer(
    # bootstrap_servers=["kafka-28c7f468-shubham-20a9.j.aivencloud.com:26262"],
    # security_protocol="SSL",
    # ssl_cafile=os.path.join(base_dir, "certs", "ca.pem"),
    # ssl_certfile=os.path.join(base_dir, "certs", "service.cert"),
    # ssl_keyfile=os.path.join(base_dir, "certs", "service.key"),
    bootstrap_servers=f"kafka-28c7f468-shubham-20a9.j.aivencloud.com:26262",
    security_protocol="SSL",
    ssl_cafile=os.path.join(base_dir, "certs", "ca.pem"),
    ssl_certfile=os.path.join(base_dir, "certs", "service.cert"),
    ssl_keyfile=os.path.join(base_dir, "certs", "service.key"),
    acks="all",
    retries=5,
    linger_ms=20,
    batch_size=32768,
    compression_type="gzip",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def delete_topic(topic_name):
    try:
        # admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' deleted successfully.")
    except UnknownTopicOrPartitionError:
        print(f"Topic '{topic_name}' does not exist and cannot be deleted.")

def publish_to_kafka(topic, message, unique_name):
    # producer.send(topic, message)
    # producer.flush()
    try:
        future = producer.send(topic, key=unique_name, value=message)
        result = future.get(timeout=30)
        logging.info(f"Sent to {result.topic}:{result.partition}@{result.offset}")
    except Exception as e:
        logging.exception(f"Kafka publish failed: {e}")
