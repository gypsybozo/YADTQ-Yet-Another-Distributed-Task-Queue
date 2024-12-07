import redis
from yadtq import create_yadtq
from yadtq.api.client import TaskClient
import time
import uuid

def check_task_status_periodically(task_ids, client, results, worker_task_counts):
    """
    Periodically checks the status of tasks and prints the results.
    """
    while task_ids:
        for task_id in task_ids[:]:
            result = client.get_result(task_id)
            if result:
                status = result.get('status', 'unknown')
                print(f"Task ID: {task_id} | Status: {status}")
                
                # If the task is completed, remove it from the list and store the result
                if status in ['success', 'failed']:
                    task_ids.remove(task_id)
                    results[task_id] = result
                    worker_id = result.get('worker_id')
                    if worker_id:
                        worker_task_counts[worker_id] = worker_task_counts.get(worker_id, 0) + 1
                    
                # Optionally, print the result of the task
                if status == 'success':
                    print(f"Result: {result.get('result', 'No result available')}")
                elif status == 'failed':
                    print(f"Error: {result.get('result', 'No error message available')}")
                
        # Sleep for a short duration before checking again
        time.sleep(5)

def main():
    broker, result_store = create_yadtq()
    client = TaskClient(broker, result_store)

    # Define tasks with intentional duplicates
    tasks = [
        ('add', (12, 3)),
        ('add', (12, 3)),  # Duplicate
        ('subtract', (101, 4)),
        ('divide', (42, 6)),
        ('divide', (42, 6)),  # Duplicate
        ('add', (50, 25)),
        ('subtract', (200, 50)),
        ('add', (30, 20)),
        ('divide', (50, 10)),
        ('subtract', (80, 30)),
        ('add', (100, 200)),
        ('divide', (15, 0))
    ]

    task_ids = []
    task_id_mapping = {}
    submission_times = {}
    worker_task_counts = {}
    results = {}

    print("\n📋 Task Submission Phase:")
    print("------------------------")

    for task_name, args in tasks:
        task_signature = f"{task_name}:{args}"
       
        if task_signature in task_id_mapping:
            print(f"\n🔄 Duplicate task detected: {task_signature}")
            task_id = task_id_mapping[task_signature]
            print(f"↪️ Reusing existing task ID: {task_id}")
           
            # Check if result already exists
            existing_result = client.get_result(task_id)
            if existing_result and existing_result['status'] == 'success':
                print(f"📝 Found cached result: {existing_result['result']}")
                continue
        else:
            task_id = client.submit(task_name, *args)
            task_id_mapping[task_signature] = task_id
            submission_times[task_id] = time.time()
           
            print(f"\n📤 Queued new task: {task_signature}")
            print(f"📎 Task ID: {task_id}")
            task_ids.append(task_id)
           
            # Small delay between submissions to help with distribution
            time.sleep(0.1)

    print("\n⚙️ Task Execution Phase:")
    print("------------------------")

    # Periodically check task statuses and print results
    check_task_status_periodically(task_ids, client, results, worker_task_counts)

    print("\n📊 Task Distribution Summary:")
    print("---------------------------")
    for worker_id, count in worker_task_counts.items():
        print(f"Worker {worker_id}: {count} tasks")

    print("\n📈 Final Results Summary:")
    print("----------------------")
    for task_id, result in results.items():
        print(f"\nTask ID: {task_id}")
        print(f"Status: {result['status']}")
        print(f"Worker: {result.get('worker_id', 'N/A')}")
        print(f"Result: {result.get('result', 'N/A')}")
        print(f"Timestamp: {result.get('timestamp', 'N/A')}")

if __name__ == "__main__":
    main()
