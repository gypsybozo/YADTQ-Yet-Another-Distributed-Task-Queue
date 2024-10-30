import uuid
from dataclasses import dataclass
from typing import Any, Dict
from datetime import datetime  # Import datetime

@dataclass
class Task:
    """Task representation"""
    task_id: str
    task_name: str
    args: tuple
    kwargs: Dict[str, Any]
    
    @classmethod
    def create(cls, task_name, *args, **kwargs):
        return cls(
            #creates unique ID -> req 1, client side support
            task_id=str(uuid.uuid4()),
            task_name=task_name,
            args=args,
            kwargs=kwargs
        )
    
    def to_dict(self):
        return {
            'task_id': self.task_id,
            'task_name': self.task_name,
            'args': self.args,
            'kwargs': self.kwargs,
            'timestamp': datetime.utcnow().isoformat()
        }
