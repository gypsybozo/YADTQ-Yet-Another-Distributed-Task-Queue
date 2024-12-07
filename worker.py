from yadtq import create_yadtq
from yadtq.api.worker import TaskWorker
import time
import threading

# Define task handlers
def add(a, b):
    time.sleep(12)
    return a + b

def subtract(a, b):
    time.sleep(18)
    return a - b

def divide(a, b):
    time.sleep(8)
    return a / b

# Create task handlers dictionary
task_handlers = {
    'add': add,
    'subtract': subtract,
    'divide': divide
}

def run_worker(worker_id):
    # Initialize YADTQ broker and result store
    broker, result_store = create_yadtq()
    
    # Start the worker with given ID
    worker = TaskWorker(worker_id, task_handlers, broker, result_store)
    print(f"Worker {worker_id} started")
    worker.start()

if __name__ == "__main__":
    # Run worker with a unique identifier
    import sys
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker_default"
    run_worker(worker_id)
