# yadtq/core/result_db.py

import redis
from datetime import datetime

class ResultStore:
    """Internal result storage implementation using Redis with deduplication"""
    def __init__(self, host='localhost', port=6379):
        self.redis_client = redis.Redis(host=host, port=port)
    
    def set_task_status(self, task_id, status, result=None, worker_id=None):
        # Use Redis transaction to ensure atomicity
        with self.redis_client.pipeline() as pipe:
            while True:
                try:
                    # Watch the task key for changes
                    pipe.watch(task_id)
                    
                    # Check if task exists and its status
                    current_status = self.redis_client.hget(task_id, 'status')
                    
                    # Don't override completed tasks
                    if current_status and current_status.decode('utf-8') in ['success', 'failed']:
                        pipe.unwatch()
                        return False
                    
                    # Start transaction
                    pipe.multi()
                    
                    mapping = {
                        'status': status,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    if result is not None:
                        mapping['result'] = str(result)
                    if worker_id is not None:
                        mapping['worker_id'] = worker_id
                        
                    pipe.hset(task_id, mapping=mapping)
                    pipe.execute()
                    return True
                    
                except redis.WatchError:
                    # Another client modified the key while we were processing
                    continue
    
    def get_task_status(self, task_id):
        task_info = self.redis_client.hgetall(task_id)
        if not task_info:
            return None
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in task_info.items()}

    def is_task_completed(self, task_id):
        """Check if task has already been completed"""
        task_info = self.get_task_status(task_id)
        if task_info and task_info.get('status') in ['success', 'failed']:
            return True
        return False

    def update_worker_heartbeat(self, worker_id):
        self.redis_client.hset(
            f'worker:{worker_id}',
            mapping={
                'last_heartbeat': datetime.utcnow().isoformat(),
                'status': 'active'
            }
        )
        self.redis_client.expire(f'worker:{worker_id}', 30)
