import sys

from django.apps import AppConfig
from kafka import KafkaConsumer

from BetData.transaction_scheduler import init_scheduler

consumer = KafkaConsumer('quickstart', bootstrap_servers='kafka:9092', auto_offset_reset='earliest')

class BetdataConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'BetData'

    # override of this method, it's called when Django app is ready to start
    def ready(self):
        if not 'migrate' in sys.argv:
            init_scheduler()

        for msg in consumer:
            print(msg)