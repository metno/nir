import zmq
import json
import core.models
import django.conf

IPC_REPLY_OK = 'OK'
IPC_REPLY_NAK = 'NAK'

class InvalidMessageException(Exception):
    pass

class Broker(object):
    def log(self, msg):
        print msg

    def __init__(self):
        self.log('Starting broker')
        self.context = zmq.Context()
        self.sock_pub = self.context.socket(zmq.PUB)
        self.sock_pub.bind(django.conf.settings.ZMQ_PUBLISHER_SOCKET)
        self.sock_ipc = self.context.socket(zmq.REP)
        self.sock_ipc.bind(django.conf.settings.ZMQ_IPC_SOCKET)
        self.log('Broker started, ready to accept connections')

    def get_dataset(self, pk):
        qs = core.models.Dataset.objects.filter(pk=pk)
        if qs.count() != 1:
            raise InvalidMessageException('can not find Dataset object with pk %d' % pk)
        return qs[0]

    def get_valid_message_pk(self, msg):
        try:
            pk = int(msg)
        except ValueError:
            raise InvalidMessageException('not an integer')
        return pk

    def publish(self, dataset):
        s = {
                'id': dataset.id,
                'model': dataset.model.id,
                'date': dataset.date.strftime('%Y-%m-%d'),
                'term': dataset.term,
                'status': dataset.status,
                'files': [f.uri for f in dataset.files.all()],
                'created_at': unicode(dataset.created_at),
                'completed_at': unicode(dataset.completed_at),
        }
        msg = json.dumps(s)
        self.sock_pub.send_string(msg)
        self.log('Published message: %s' % msg)

    def main(self):
        while True:
            msg = self.sock_ipc.recv_string()
            self.log('Received message: %s' % msg)

            try:
                pk = self.get_valid_message_pk(msg)
                dataset = self.get_dataset(pk)
            except InvalidMessageException, e:
                self.log('Error processing message: %s' % unicode(e))
                self.sock_ipc.send_string(IPC_REPLY_NAK)
                continue

            self.publish(dataset)

            self.sock_ipc.send_string(IPC_REPLY_OK)


def send_ipc_message(msg):
    reply = None
    context = zmq.Context()

    sock = context.socket(zmq.REQ)
    sock.connect(django.conf.settings.ZMQ_IPC_SOCKET)

    poll = zmq.Poller()
    poll.register(sock, zmq.POLLIN)

    sock.send_string(unicode(msg))
    socks = dict(poll.poll(django.conf.settings.ZMQ_IPC_REQUEST_TIMEOUT))
    if socks.get(sock) == zmq.POLLIN:
        reply = sock.recv_string()

    sock.setsockopt(zmq.LINGER, 0)
    sock.close()
    poll.unregister(sock)
    context.term()

    if reply == IPC_REPLY_OK:
        return True
    return False
