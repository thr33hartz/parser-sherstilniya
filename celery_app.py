import os
from dotenv import load_dotenv
from celery import Celery

# Загружаем переменные из .env файла
load_dotenv()

# Получаем URL для подключения к Redis
REDIS_URL = os.getenv("REDIS_URL")

# Проверка на случай, если забыли добавить URL в .env
if not REDIS_URL:
    raise ValueError("Необходимо установить REDIS_URL в вашем .env файле!")

# Создаем экземпляр Celery, используя URL из .env
celery = Celery(
    'tasks',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['tasks.celery_tasks']
)

# Опциональные настройки
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)