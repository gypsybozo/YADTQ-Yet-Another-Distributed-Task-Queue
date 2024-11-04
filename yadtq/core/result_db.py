from datetime import datetime
from collections import defaultdict
import redis

class ResultStore:
    """Internal result storage implementation using Redis"""
    def __init__(self, host='localhost', port=6379):
        self.redis_client = redis.Redis(host=host, port=port)
    
    def set_task_status(self, task_id, status, result=None, worker_id=None):
        mapping = {
            'status': status,
            'timestamp': datetime.utcnow().isoformat()
        }
        if result is not None:
            mapping['result'] = str(result)
        if worker_id is not None:
            mapping['worker_id'] = worker_id
            
        self.redis_client.hset(task_id, mapping=mapping)

    def get_task_status(self, task_id):
        task_info = self.redis_client.hgetall(task_id)
        if not task_info:
            return None
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in task_info.items()}

    def update_worker_heartbeat(self, worker_id):
        self.redis_client.hset(
            f'worker:{worker_id}',
            mapping={
                'last_heartbeat': datetime.utcnow().isoformat(),
                'status': 'active'
            }
        )
        self.redis_client.expire(f'worker:{worker_id}', 30)

    def get_all_task_ids(self, status_filter=None):
        task_ids = []
        for key in self.redis_client.keys('*'):
            task_info = self.redis_client.hgetall(key)
            status = task_info.get(b'status', b'').decode('utf-8')
            if status_filter is None or status == status_filter:
                task_ids.append(key.decode('utf-8'))
        return task_ids

    def get_all_worker_ids(self):
        return [key.decode('utf-8').split(':')[1] for key in self.redis_client.keys('worker:*')]

    def get_worker_status(self, worker_id):
        return self.redis_client.hgetall(f'worker:{worker_id}')
