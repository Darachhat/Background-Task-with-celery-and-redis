import os
from datetime import datetime

from celery import Celery
from celery.schedules import crontab, schedule

app = Celery(
    'ticker',
    broker = os.getenv('CELERY_BROKER_URL'),
    backend = os.getenv('CELERY_BACKEND_URL')
)

OUTPUT_PATH = "/data/timestamps.txt"


@app.task
def write_timestamp():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()}\n")


# schedule every 10 seconds
app.conf.beat_schedule = {
    "timestamp_writer": {
        "task": "worker.write_timestamp", # name of the task
        "schedule": schedule(10.0),  # every 10 seconds
    }
}

# schedule every day at specified time
# app.conf.beat_schedule = {
#     "nightly_job": {
#         "task": "worker.write_timestamp", # name of the task
#         "schedule": crontab(minute=25, hour=15), # every day at 15:25 Phnom Penh time
#     }
# }

app.conf.timezone = 'Asia/Phnom_Penh'  # set timezone to Phnom Penh
app.conf.enable_utc = True  # enable UTC time

