# yadtq/api/worker.py

import threading
from typing import Dict, Callable
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskWorker:
    """Public worker API for processing tasks"""
    def __init__(self, worker_id: str, task_handlers: Dict[str, Callable],
                 broker, result_store):
        self.worker_id = worker_id
        self.task_handlers = task_handlers
        self._broker = broker
        self._result_store = result_store
        self._consumer = self._broker.get_consumer('yadtq_worker_group')
        self._running = False
        self._heartbeat_thread = None

    def _send_heartbeat(self):
        """Internal method to send periodic heartbeats"""
        while self._running:
            self._result_store.update_worker_heartbeat(self.worker_id)
            logger.info(f"{self.worker_id} sending heartbeat.")
            time.sleep(10)

    def _process_task(self, task_data):
        """Internal method to process a single task with deduplication"""
        task_id = task_data['task_id']
        
        # Check if task is already completed
        if self._result_store.is_task_completed(task_id):
            logger.info(f"{self.worker_id}: Task {task_id} already completed, skipping")
            return
            
        # Try to claim the task
        success = self._result_store.set_task_status(
            task_id, 'processing', worker_id=self.worker_id
        )
        
        if not success:
            logger.info(f"{self.worker_id}: Task {task_id} already being processed by another worker")
            return
            
        logger.info(f"{self.worker_id} processing task {task_id}: {task_data['task_name']}")
        
        try:
            handler = self.task_handlers[task_data['task_name']]
            result = handler(*task_data['args'], **task_data['kwargs'])
            self._result_store.set_task_status(task_id, 'success', result)
            logger.info(f"{self.worker_id} completed task {task_id} successfully: {result}")
        except Exception as e:
            self._result_store.set_task_status(task_id, 'failed', str(e))
            logger.error(f"{self.worker_id} failed to process task {task_id}: {str(e)}")

    def start(self):
        """Start processing tasks"""
        self._running = True
        
        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(target=self._send_heartbeat)
        self._heartbeat_thread.daemon = True
        self._heartbeat_thread.start()
        
        logger.info(f"{self.worker_id} started processing tasks.")
        
        # Process tasks
        try:
            while self._running:
                messages = self._consumer.poll(timeout_ms=1000)
                logger.info(f"{self.worker_id} is polling for tasks...")
                
                if not messages:
                    logger.info(f"{self.worker_id} found no new messages.")
                
                for topic_partition, batch in messages.items():
                    
                    logger.info(f"{self.worker_id} assigned to partition {topic_partition.partition}")
                    for message in batch:
                        task_data = message.value
                        self._process_task(task_data)
                        self._consumer.commit()
        finally:
            self._running = False
            if self._heartbeat_thread:
                self._heartbeat_thread.join(timeout=1)
            logger.info(f"{self.worker_id} has stopped processing tasks.")

    def stop(self):
        """Stop processing tasks"""
        self._running = False
