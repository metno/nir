import django.core.management.base
import zmq
import json
import core.models

REPLY_OK = 'OK'
REPLY_NAK = 'NAK'

class InvalidMessageException(Exception):
    pass

class Broker(object):
    def log(self, msg):
        print msg

    def bootstrap(self):
        self.log('Starting broker')
        self.context = zmq.Context()
        self.sock_pub = self.context.socket(zmq.PUB)
        self.sock_pub.bind('tcp://*:5555')
        self.sock_ipc = self.context.socket(zmq.REP)
        self.sock_ipc.bind('ipc:///tmp/zmq')
        self.log('Broker started, ready to accept connections')

    def extract_message_object(self, msg):
        try:
            pk = int(msg)
        except ValueError:
            raise InvalidMessageException('not an integer')
        qs = core.models.WeatherModelStatus.objects.filter(pk=pk)
        if qs.count() != 1:
            raise InvalidMessageException('can not find WeatherModelStatus object with pk %d' % pk)
        return qs[0]

    def publish(self, status):
        dataset = {
                'model': status.weathermodelrun.weathermodel.id,
                'date': status.weathermodelrun.date.strftime('%Y-%m-%d'),
                'term': status.weathermodelrun.term,
                'status': status.status,
                'datetime': unicode(status.datetime)
        }
        msg = json.dumps(dataset)
        self.sock_pub.send_string(msg)
        self.log('Published message: %s' % msg)

    def run(self):
        while True:
            msg = self.sock_ipc.recv_string()
            self.log('Received message: %s' % msg)

            try:
                status = self.extract_message_object(msg)
            except InvalidMessageException, e:
                self.log('Error processing message: %s', unicode(e))
                self.sock_ipc.send_string(REPLY_NAK)
                continue

            self.publish(status)

            self.sock_ipc.send_string(REPLY_OK)


class Command(django.core.management.base.BaseCommand):
    help = 'Runs the core communication module'

    def handle(self, *args, **options):
        broker = Broker()
        broker.bootstrap()
        broker.run()

