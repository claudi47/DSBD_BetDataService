import sys

from django.apps import AppConfig

from BetData.transaction_scheduler import init_scheduler

class BetdataConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'BetData'

    # override of this method, it's called when Django app is ready to start
    def ready(self):
        if not 'migrate' in sys.argv:
            init_scheduler()
