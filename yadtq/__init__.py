# yadtq/__init__.py or yadtq/api/__init__.py

from .core.broker import MessageBroker  # Assuming you have a MessageBroker class
from .core.result_db import ResultStore  # Assuming you have a ResultStore class

def create_yadtq(kafka_servers=['localhost:9092'], 
                 redis_host='localhost', redis_port=6379):
    """Create YADTQ broker and result store instances."""
    # Initialize the message broker with the specified Kafka servers
    broker = MessageBroker(kafka_servers)
    
    # Initialize the result store with the specified Redis connection details
    result_store = ResultStore(redis_host, redis_port)
    
    # Return both the broker and result store for use in task processing and result management
    return broker, result_store
