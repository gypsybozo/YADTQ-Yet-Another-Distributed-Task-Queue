from typing import Any, Dict
import time
from yadtq.core.task import Task


class TaskClient:
    """Public client API for submitting tasks and checking results"""
    def __init__(self, broker, result_store):
        self._broker = broker
        self._result_store = result_store
        self._producer = self._broker.get_producer()

    def submit(self, task_name: str, *args, **kwargs) -> str:
        """Submit a task to the queue"""
        task = Task.create(task_name, *args, **kwargs)
        
        # Initialize task status as queued -> req 1 client side
        self._result_store.set_task_status(task.task_id, 'queued')
        
        # Send task to queue
        self._producer.send(self._broker.topic, task.to_dict())
        self._producer.flush()
        
        return task.task_id

    def get_result(self, task_id: str) -> Dict[str, Any]:
        """Get the status and result of a task"""
        result = self._result_store.get_task_status(task_id)
        if not result:
            return {'status': 'not_found'}
        return result

    def wait_for_result(self, task_id: str, timeout: int = None) -> Dict[str, Any]:
        """Wait for task completion with optional timeout"""
        start_time = time.time()
        while True:
            result = self.get_result(task_id)
            if result['status'] in ['success', 'failed']:
                return result
            if timeout and time.time() - start_time > timeout:
                return {'status': 'timeout'}
            time.sleep(0.5)
