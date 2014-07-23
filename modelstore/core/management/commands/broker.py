import django.core.management.base
import core.lib.zeromq

from django.conf import settings

class Command(django.core.management.base.BaseCommand):
    help = 'Run the ZMQ broker'

    def handle(self, *args, **options):
        broker = core.lib.zeromq.Broker()
        broker.main()

