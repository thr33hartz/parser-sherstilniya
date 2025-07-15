from celery import Celery
import random
import os
from dotenv import load_dotenv
import json

load_dotenv()

celery = Celery('celery_app', broker=os.getenv('REDIS_URL'))

from tasks.celery_tasks import run_all_in_parse_periodic_task

celery.conf.timezone = 'UTC'
celery.conf.broker_connection_retry_on_startup = True
celery.conf.worker_cancel_long_running_tasks_on_connection_loss = True

# Фикс BrokenPipe
celery.conf.broker_heartbeat = 0
celery.conf.broker_transport_options = {
    'visibility_timeout': 7200,  # 2 часа на задачу
    'socket_timeout': 60,
    'socket_connect_timeout': 60,
    'socket_keepalive': True,
    'retry_on_timeout': True,
}

ALL_IN_TEMPLATE = json.loads(os.getenv('ALL_IN_TEMPLATE_JSON', '{"time_period": "24h", "platforms": [], "categories": ["completed", "completing"]}'))

# Запуск первой задачи при старте Celery
@celery.on_after_configure.connect
def setup_periodic_task(sender, **kwargs):
    sender.send_task("tasks.celery_tasks.run_all_in_parse_periodic_task", args=(ALL_IN_TEMPLATE,), countdown=0)

celery.autodiscover_tasks(['tasks.celery_tasks'])

print("Celery app loaded successfully")