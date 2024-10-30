import redis
from datetime import datetime

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