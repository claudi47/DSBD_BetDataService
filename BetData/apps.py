import multiprocessing
import sys

from confluent_kafka import Consumer
from django.apps import AppConfig

from BetData.transaction_scheduler import init_scheduler


def kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'LOLconsumer'
    })
    consumer.subscribe(['test'])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue
        print(msg.value().decode('utf-8'))
    consumer.close()


class BetdataConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'BetData'

    # override of this method, it's called when Django app is ready to start
    def ready(self):
        if not 'migrate' in sys.argv:
            init_scheduler()
            multiprocessing.Process(target=kafka_consumer).start()