import time
from datetime import datetime
from collections import defaultdict

class YADTQMonitor:
    def __init__(self, result_store):
        self.result_store = result_store
        self.queued_tasks = {}
        self.running_tasks = {}
        self.worker_stats = defaultdict(lambda: {'tasks_executed': 0, 'last_heartbeat': None})
        self.failed_workers = []

    def update_task_queue(self):
        """Update the list of queued tasks"""
        for task_id in self.result_store.get_all_task_ids(status_filter='queued'):
            status = self.result_store.get_task_status(task_id)
            self.queued_tasks[task_id] = status['timestamp']

    def update_running_tasks(self):
        """Update the list of tasks currently being processed"""
        for task_id in self.result_store.get_all_task_ids(status_filter='processing'):
            status = self.result_store.get_task_status(task_id)
            self.running_tasks[task_id] = {
                'worker_id': status['worker_id'],
                'start_time': status['timestamp']
            }

    def update_worker_stats(self):
        """Update worker statistics"""
        for worker_id in self.result_store.get_all_worker_ids():
            status = self.result_store.get_worker_status(worker_id)
            if status:
                self.worker_stats[worker_id]['tasks_executed'] += 1
                # Safely retrieve 'last_heartbeat' if it exists
                self.worker_stats[worker_id]['last_heartbeat'] = status.get('last_heartbeat', 'N/A')
            else:
                # Worker has failed
                self.failed_workers.append({
                    'worker_id': worker_id,
                    'last_heartbeat': self.worker_stats[worker_id].get('last_heartbeat', 'Unknown'),
                    'failure_time': datetime.utcnow().isoformat()
                })
                del self.worker_stats[worker_id]


    def display_monitoring_data(self):
        """Display the monitoring data"""
        print("YADTQ Monitoring Dashboard")
        print("Tasks in Queue:")
        for task_id, timestamp in self.queued_tasks.items():
            print(f"- Task ID: {task_id}, Queued at: {timestamp}")
        print()

        print("Tasks in Progress:")
        for task_id, task_info in self.running_tasks.items():
            print(f"- Task ID: {task_id}, Worker: {task_info['worker_id']}, Started: {task_info['start_time']}")
        print()

        print("Worker Statistics:")
        for worker_id, stats in self.worker_stats.items():
            print(f"- Worker ID: {worker_id}, Tasks Executed: {stats['tasks_executed']}, Last Heartbeat: {stats['last_heartbeat']}")
        print()

        print("Failed Workers:")
        for worker_info in self.failed_workers:
            print(f"- Worker ID: {worker_info['worker_id']}, Last Heartbeat: {worker_info['last_heartbeat']}, Failed at: {worker_info['failure_time']}")
        print()

    def run(self):
        """Main monitoring loop"""
        while True:
            self.update_task_queue()
            self.update_running_tasks()
            self.update_worker_stats()
            self.display_monitoring_data()
            time.sleep(5)