# broker.py
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

class MessageBroker:
    """Internal message broker implementation using Kafka with proper load balancing"""
   
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'yadtq_tasks'
        self._ensure_topic_exists()

    def _ensure_topic_exists(self):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            if self.topic not in admin_client.list_topics():
                # Create topic with multiple partitions
                topic = NewTopic(
                    name=self.topic,
                    num_partitions=4,  # One partition per worker
                    replication_factor=1
                )
                admin_client.create_topics([topic])
        except Exception as e:
            print(f"Warning: Could not create topic: {e}")

    def get_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Let Kafka handle partitioning
            key_serializer=lambda v: str(v).encode('utf-8') if v else None
        )

    def get_consumer(self, group_id):
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id='yadtq_workers',  # Use same group for all workers
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=False,  # Enable auto commit for better load balancing
            auto_offset_reset='earliest',
            session_timeout_ms=10000,  # Increase timeout for better stability
            heartbeat_interval_ms=3000
        )
	
