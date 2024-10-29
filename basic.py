from yadtq import create_yadtq
from yadtq.api.client import TaskClient
from yadtq.api.worker import TaskWorker
import threading
import time

# Define task handlers
def add(a, b):
    time.sleep(2)  # Simulate work
    return a + b

def multiply(a, b):
    time.sleep(2)  # Simulate work
    return a * b

# Create task handlers dictionary
task_handlers = {
    'add': add,
    'multiply': multiply
}

def run_worker(worker_id, broker, result_store):
    worker = TaskWorker(worker_id, task_handlers, broker, result_store)
    worker.start()

def main():
    # Create YADTQ instance
    broker, result_store = create_yadtq()
    
    # Create client
    client = TaskClient(broker, result_store)

    # Submit tasks
    tasks = [
        ('add', (5, 3)),
        ('multiply', (4, 6))
    ]

    task_ids = []  # List to store submitted task IDs

    # Submit tasks and store their IDs
    for task_name, args in tasks:
        task_id = client.submit(task_name, *args)
        task_ids.append(task_id)
        print(f"Submitted {task_name}{args} with ID: {task_id}")
    # Start workers
    worker_threads = []
    for i in range(3):
        thread = threading.Thread(
            target=run_worker,
            args=(f"worker_{i}", broker, result_store)
        )
        thread.daemon = True
        thread.start()
        worker_threads.append(thread)
    
    # Give workers a moment to start processing
    time.sleep(1)
    # Wait for results
    results = {}
    while task_ids:
        for task_id in task_ids[:]:  # Create a copy of task_ids to iterate over
            try:
                # Check if the result is available
                result = client.wait_for_result(task_id)  # Assume there's a method to check for results
                if result is not None:  # If the result is available
                    print(f"Task {task_id} completed: {result}")
                    results[task_id] = result
                    task_ids.remove(task_id)  # Remove the task ID from the list
            except Exception as e:
                print(f"Error checking task {task_id}: {e}")
        
        time.sleep(1)  # Wait a bit before checking again

    print("All tasks completed.")

if __name__ == "__main__":
    main()