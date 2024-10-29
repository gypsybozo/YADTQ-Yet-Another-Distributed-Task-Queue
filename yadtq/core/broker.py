import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

class MessageBroker:
    """Internal message broker implementation using Kafka"""
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'yadtq_tasks'
        self._ensure_topic_exists()
        
    def _ensure_topic_exists(self):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            if self.topic not in admin_client.list_topics():
                topic = NewTopic(name=self.topic, num_partitions=3, replication_factor=1)
                admin_client.create_topics([topic])
        except Exception as e:
            print(f"Warning: Could not create topic: {e}")

    def get_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def get_consumer(self, group_id):
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=False
        )