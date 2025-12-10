# Celery + RabbitMQ + PostgreSQL Background Task System - Implementation Guide

## Project Overview

Built a production-ready distributed background task system using Celery with RabbitMQ as message broker and PostgreSQL as result backend. The system demonstrates periodic task scheduling, fault tolerance, crash recovery, and horizontal scalability.

## Architecture Components

### Core Stack

- **Celery 5.6.0**: Distributed task queue with Beat scheduler for periodic tasks
- **RabbitMQ 3-management**: AMQP message broker with management UI (port 15673)
- **PostgreSQL 16-alpine**: Result backend storing task states and results
- **Docker Compose**: Multi-container orchestration with health checks
- **Python 3.12-slim**: Runtime environment

### System Design

```
Celery Beat (Scheduler)
    ↓ (every 10 seconds)
RabbitMQ Exchange ('celery')
    ↓ (routing_key='celery')
RabbitMQ Queue ('celery', durable)
    ↓
Celery Worker (prefork, 20 workers)
    ↓
Task Execution → PostgreSQL (results) + File Output
```

## Key Implementation Details

### 1. Worker Configuration (`worker.py`)

```python
from celery import Celery
from datetime import datetime
import os

# Initialize Celery app
app = Celery(
    'worker',
    broker=os.getenv('CELERY_BROKER_URL'),
    backend=os.getenv('CELERY_BACKEND_URL')
)

# Centralized configuration
app.conf.update(
    timezone='Asia/Phnom_Penh',
    enable_utc=True,

    # Periodic task schedule
    beat_schedule={
        'write-timestamp-every-10-seconds': {
            'task': 'worker.write_timestamp',
            'schedule': 10.0,
        },
    },

    # Reliability settings
    task_acks_late=True,                    # Acknowledge after task completion
    task_reject_on_worker_lost=True,        # Requeue on worker crash
    worker_prefetch_multiplier=1,           # One task per worker at a time

    # Broker connection retry
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10,

    # Message persistence
    task_default_delivery_mode='persistent',

    # Queue configuration
    task_queues={
        'celery': {
            'exchange': 'celery',
            'routing_key': 'celery',
            'durable': True,
        }
    },
)

OUTPUT_PATH = "/data/timestamps.txt"

@app.task(name='worker.write_timestamp')
def write_timestamp():
    """Write current UTC timestamp to file"""
    timestamp = datetime.utcnow().isoformat()
    with open(OUTPUT_PATH, "a") as f:
        f.write(f"{timestamp}\n")
    return timestamp
```

### 2. Docker Compose Setup

```yaml
version: '3.8'

services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - '5672:5672'
      - '15672:15672'
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASSWORD}
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: rabbitmq-diagnostics -q ping
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped # Auto-restart on failure

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: celery
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ['CMD-SHELL', 'pg_isready -U celery -d celery']
      interval: 10s
      timeout: 5s
      retries: 5

  celery_worker:
    build: .
    command: celery -A worker worker --loglevel=info
    volumes:
      - ./data:/data
      - ./worker.py:/app/worker.py
    environment:
      CELERY_BROKER_URL: ${CELERY_BROKER_URL}
      CELERY_BACKEND_URL: ${CELERY_BACKEND_URL}
    depends_on:
      rabbitmq:
        condition: service_healthy
      postgres:
        condition: service_healthy

  celery_beat:
    build: .
    command: celery -A worker beat --loglevel=info
    volumes:
      - ./worker.py:/app/worker.py
    environment:
      CELERY_BROKER_URL: ${CELERY_BROKER_URL}
      CELERY_BACKEND_URL: ${CELERY_BACKEND_URL}
    depends_on:
      - celery_worker

volumes:
  rabbitmq_data:
  postgres_data:
```

### 3. Environment Configuration (`.env`)

```env
# RabbitMQ Configuration
RABBITMQ_USER=admin
RABBITMQ_PASSWORD=admin123

# PostgreSQL Configuration
POSTGRES_USER=celery
POSTGRES_PASSWORD=celery123

# Celery URLs
CELERY_BROKER_URL=amqp://admin:admin123@rabbitmq:5672//
CELERY_BACKEND_URL=db+postgresql://celery:celery123@postgres:5432/celery
```

### 4. Dependencies (`requirements.txt`)

```
celery[amqp]
psycopg2-binary
sqlalchemy
```

## Production-Ready Features

### Reliability & Fault Tolerance

1. **Durable Queues**: Messages persist through broker restarts
2. **Persistent Delivery**: Messages saved to disk before acknowledgment
3. **Late Acknowledgment**: Tasks acknowledged only after successful completion
4. **Worker Lost Handling**: Tasks requeued if worker crashes mid-execution
5. **Connection Retry**: Exponential backoff with max 10 retries (2s, 4s, 6s, 8s...)
6. **Auto-restart**: RabbitMQ service configured with `restart: unless-stopped`

### Message Flow & Routing

```
Producer (Beat/Manual)
    ↓ publish task with routing_key='celery'
Exchange (type=direct, name='celery')
    ↓ route based on routing_key match
Queue (name='celery', durable=true)
    ↓ deliver to available worker
Consumer (Celery Worker)
    ↓ execute task
Result Backend (PostgreSQL)
```

**Exchange Types Explained:**

- **Direct**: Routes to queues with exact routing_key match (used in this project)
- **Fanout**: Broadcasts to all bound queues (ignore routing_key)
- **Topic**: Pattern-based routing (e.g., `logs.*.error`)
- **Headers**: Routes based on message header attributes

## Crash Recovery Testing Results

### Test Scenario

1. RabbitMQ killed at: `09:16:15`
2. Worker reconnection at: `09:16:39`
3. Downtime duration: **24 seconds**
4. Missed tasks: **2 tasks** (09:16:21, 09:16:31)

### Recovery Timeline

```
09:16:11 ✓ Task executed successfully
09:16:15 ✗ RabbitMQ killed (docker compose kill rabbitmq)
09:16:21 ✗ Missed (Beat couldn't queue - broker down)
09:16:31 ✗ Missed (Beat couldn't queue - broker down)
09:16:39 ✓ Worker reconnected, execution resumed
09:16:41 ✓ Task executed successfully
09:16:51 ✓ Normal operation continues
```

### Key Findings

- **Worker auto-reconnection works perfectly** using exponential backoff
- **Queued tasks are preserved** due to durable queues + persistent messages
- **Beat scheduler does NOT buffer missed tasks** during downtime
- Tasks scheduled while broker is unavailable are **permanently lost**
- Recovery is automatic, no manual intervention required

## Scalability

### Horizontal Scaling

```bash
# Scale workers to 5 instances
docker compose up -d --scale celery_worker=5

# Each worker:
# - Connects to same RabbitMQ broker
# - Pulls tasks from shared queue
# - Stores results in shared PostgreSQL
# - Processes tasks independently
```

**Benefits:**

- Increased throughput for task processing
- Better resource utilization
- Load distribution across workers
- No code changes required

## Integration with FastAPI

### Basic Integration Pattern

```python
from fastapi import FastAPI, BackgroundTasks
from celery import Celery
import os

app = FastAPI()

# Initialize Celery
celery_app = Celery(
    'tasks',
    broker=os.getenv('CELERY_BROKER_URL'),
    backend=os.getenv('CELERY_BACKEND_URL')
)

# Configure Celery (similar to worker.py)
celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
    task_default_delivery_mode='persistent',
)

@celery_app.task(name='tasks.process_data')
def process_data(data: dict):
    """Background task for heavy processing"""
    # Your processing logic here
    return {"status": "completed", "data": data}

@app.post("/submit-task")
async def submit_task(data: dict):
    """Endpoint to submit background task"""
    task = process_data.delay(data)
    return {
        "task_id": task.id,
        "status": "submitted"
    }

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Check task execution status"""
    task = celery_app.AsyncResult(task_id)
    return {
        "task_id": task_id,
        "status": task.state,
        "result": task.result if task.ready() else None
    }
```

### Periodic Task Example

```python
# In worker.py or tasks.py
celery_app.conf.beat_schedule = {
    'cleanup-old-data': {
        'task': 'tasks.cleanup_old_data',
        'schedule': 3600.0,  # Every hour
    },
    'send-daily-report': {
        'task': 'tasks.send_daily_report',
        'schedule': crontab(hour=9, minute=0),  # 9 AM daily
    },
}

@celery_app.task(name='tasks.cleanup_old_data')
def cleanup_old_data():
    """Clean up old records from database"""
    # Your cleanup logic
    pass

@celery_app.task(name='tasks.send_daily_report')
def send_daily_report():
    """Generate and send daily reports"""
    # Your report logic
    pass
```

## Commands Reference

### Start System

```bash
docker compose up -d
```

### View Logs

```bash
docker compose logs -f celery_worker
docker compose logs -f celery_beat
docker compose logs -f rabbitmq
```

### Monitor RabbitMQ

- Management UI: http://localhost:15672
- Login: admin / admin123

### Scale Workers

```bash
docker compose up -d --scale celery_worker=3
```

### Test Crash Recovery

```bash
# Kill RabbitMQ
docker compose kill rabbitmq

# Watch worker reconnection
docker compose logs -f celery_worker

# Restart manually (or wait for auto-restart)
docker compose start rabbitmq
```

### Stop System

```bash
docker compose down
docker compose down -v  # Remove volumes too
```

## Lessons Learned

### Critical Insights

1. **Celery Beat doesn't buffer missed schedules** - tasks scheduled during broker downtime are lost permanently
2. **Worker reconnection is automatic** with proper retry configuration
3. **Durable queues + persistent messages** ensure queued tasks survive broker restarts
4. **Health checks are essential** for proper service orchestration
5. **Late acknowledgment prevents task loss** on worker crashes

### Best Practices for Production

1. **Use RabbitMQ clustering** for high availability (avoid single point of failure)
2. **Monitor task execution** with tools like Flower or custom monitoring
3. **Implement task idempotency** to handle retries safely
4. **Set task timeouts** to prevent hung tasks: `task_time_limit=300`
5. **Use task priorities** for critical tasks: `task_default_priority=5`
6. **Configure result expiration**: `result_expires=3600` (1 hour)
7. **Add logging and alerting** for missed schedules or failures
8. **Test failure scenarios** regularly in staging environment

### Advanced Reliability Features

```python
app.conf.update(
    # Task execution limits
    task_time_limit=300,              # Hard limit: 5 minutes
    task_soft_time_limit=240,         # Soft limit: 4 minutes (raises exception)

    # Retry configuration
    task_autoretry_for=(Exception,),  # Auto-retry on exceptions
    task_retry_kwargs={'max_retries': 3, 'countdown': 60},

    # Result backend settings
    result_expires=3600,              # Results expire after 1 hour
    result_backend_transport_options={'master_name': 'mymaster'},

    # Task routing
    task_routes={
        'tasks.heavy_task': {'queue': 'heavy'},
        'tasks.quick_task': {'queue': 'quick'},
    },
)
```

## Summary

This implementation demonstrates a **production-ready distributed task queue** with:

- ✅ Automatic crash recovery
- ✅ Message persistence and durability
- ✅ Horizontal scalability
- ✅ Connection retry with exponential backoff
- ✅ Reliable task execution with late acknowledgment
- ✅ PostgreSQL result backend for task state tracking
- ✅ Docker-based deployment with health checks
- ✅ Periodic task scheduling with Celery Beat

**Use this as a template for integrating background tasks in your FastAPI backend**, adapting the task definitions and schedules to your specific business requirements.
