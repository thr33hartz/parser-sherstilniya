# services/queue_service.py

import redis
import config

def get_queue_length(queue_name='celery') -> int:
    """
    Подключается к Redis и возвращает текущую длину очереди Celery.
    """
    try:
        # Создаем подключение к Redis, используя URL из нашего конфига
        r = redis.from_url(config.REDIS_URL)

        # Команда LLEN возвращает длину списка (нашей очереди)
        length = r.llen(queue_name)
        return length
    except Exception as e:
        print(f"ERROR: Не удалось подключиться к Redis или получить длину очереди: {e}")
        return 0 # В случае ошибки возвращаем 0