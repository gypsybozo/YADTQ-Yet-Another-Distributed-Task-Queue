# YADTQ-Yet-Another-Distributed-Task-Queue-
A distributed task queue implementation using Kafka as the message broker, featuring task deduplication, worker load balancing, and result caching.

## Features
- Task deduplication to prevent redundant processing
- Worker load balancing across multiple Kafka partitions
- Result caching for improved performance
- Worker health monitoring via heartbeats
- Asynchronous task processing with status tracking
- Fault tolerance and task recovery
- Support for multiple concurrent workers

## Prerequisites
- Python 3.7+
- Apache Kafka
- Redis (for result storage)

## Installation
```bash
pip install -r requirements.txt
```

## Quick Start
### 1. Start Workers
Run multiple worker instances with unique IDs:

```bash
python worker.py worker1
python worker.py worker2
```
### 2. Submit Tasks
```bash
python basic.py
```
## Task Handlers
- Basic tasks like addition, subtraction, multiplication and division with sleep times in between are defined to showcase processing of different tasks.

## Components

### TaskClient: Handles task submission and result retrieval
- Prevent duplication of tasks
- Result caching
- Status tracking

### TaskWorker: Processes tasks from the queue
- Heartbeat monitoring
- Task claiming mechanism
- Result caching
- Error handling and reporting

### MessageBroker: Manages task distribution
- Kafka-based implementation
- Multiple partition support
- Load balancing across workers

  ## Task Lifecycle
- Client submits task
- Task is queued with 'queued' status
- Worker claims task and sets status to 'processing'
- Task is executed and result is stored
- Final status set to 'success' or 'failed'

## Configuration
The system uses default configurations for local development:
- **Kafka Bootstrap Servers**: localhost:9092
- **Topic**: yadtq_tasks
- **Number of Partitions**: 4
- **Consumer Group**: yadtq_worker_group

## Monitoring
The system provides several monitoring features:
- Worker heartbeat tracking
- Task status monitoring
- Worker task distribution statistics
- Execution time tracking
- Error logging and reporting

## Error Handling
- Failed tasks are marked with 'failed' status
- Error messages are stored in the result store
- Workers automatically recover from failures
- Task timeouts are supported

## Best Practices
- Always use unique worker IDs
- Configure appropriate timeouts for your use case

