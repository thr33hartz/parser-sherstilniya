import asyncio
from celery import Celery
from celery.schedules import crontab

celery = Celery('tasks', broker='redis://default:cieY30ShEQ0tt2EBVTDIUU90dVOgrtvq@redis-12393.c62.us-east-1-4.ec2.redns.redis-cloud.com:12393')  # Укажите ваш брокер (Redis, RabbitMQ и т.д.)

@app.task
def run_automatic_all_in_parse(chat_id: int):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(automatic_all_in_parse(chat_id))

# Настройка расписания
app.conf.beat_schedule = {
    'run-automatic-all-in-parse': {
        'task': 'tasks.run_automatic_all_in_parse',
        'schedule': crontab(hour=0, minute=0),  # Запуск каждый день в полночь
        'args': (YOUR_CHAT_ID,),  # Замените на реальный chat_id пользователя
    },
}