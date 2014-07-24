import zmq
import json

ZMQ_SOCKET = 'tcp://localhost:5555'

if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    print 'Connecting to upstream...',
    socket.connect(ZMQ_SOCKET)
    print 'connected'

    socket.setsockopt_string(zmq.SUBSCRIBE, u'')

    while True:
        s = socket.recv_string()
        dataset = json.loads(s)
        print 'Upstream reports that %s term %d has changed status to %d' % (
                dataset['model'],
                dataset['term'],
                dataset['status']
                )
