"""
Syncer ZMQ module.

Syncer runs a ZMQ subscriber that listens to events from Modelstatus.
"""

import zmq
import logging


class ZMQEvent(object):
    def __init__(self, **kwargs):
        [setattr(self, key, value) for key, value in kwargs.iteritems()]

    def __repr__(self):
        return "ZeroMQ event resource=%s id=%d" % (self.resource, self.id)


class ZMQSubscriber(object):
    def __init__(self, addr):
        self.context = zmq.Context()
        self.sock = self.context.socket(zmq.SUB)
        self.sock.connect(addr)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, u'')
        self.poller = zmq.Poller()
        self.poller.register(self.sock, zmq.POLLIN)

    def recv(self):
        """
        Receive incoming messages, and return them.
        """
        try:
            msg = self.sock.recv(zmq.NOBLOCK)
        except zmq.ZMQError:
            msg = None
        return msg

    def get_event(self):
        """
        Check if there are valid resource update events in the ZeroMQ pipeline.
        Returns the result of decode_event.
        """
        msg = self.recv()
        return self.decode_event(msg)

    def get_event_timeout(self, timeout):
        """
        Like get_event(), but with a timeout in seconds.
        """
        events = self.poller.poll(timeout * 1000)
        if not events:
            return None
        return self.get_event()

    def decode_event(self, msg):
        """
        Check for valid resource update events, and return a dictionary.
        Returns None if no valid event is detected.
        """
        try:
            resource, id = msg.split(' ')
            id = int(id)
        except:
            return None
        event_dict = {
            'id': id,
            'resource': resource
        }
        event = ZMQEvent(**event_dict)
        return event


class ZMQController(object):
    """
    API to Syncer providing status queries and command issuing.
    """

    STATUS_OK = 0
    STATUS_FAIL = 1
    STATUS_INVALID = 2

    def __init__(self, addr):
        self.context = zmq.Context()
        self.init_sock(addr)
        self.poller = zmq.Poller()
        self.poller.register(self.sock, zmq.POLLIN)

    def init_sock(self, addr):
        """
        Initialize the external facing socket, listening for commands from the admin.
        """
        self.sock = self.context.socket(zmq.REP)
        self.sock.bind(addr)

    def make_reply(self, code, data):
        return {
            'status': code,
            'data': data
        }

    def run_hello(self):
        """
        Hello World command.
        """
        return self.make_reply(self.STATUS_OK, ['Hello, world!'])

    def exec_command(self, tokens):
        try:
            if len(tokens) == 0:
                raise Exception('Empty command list')
            if tokens[0] == 'hello':
                return self.run_hello()
            raise Exception('Invalid command')
        except Exception, e:
            return self.make_reply(self.STATUS_INVALID, [unicode(e)])

    def run(self):
        """
        Main loop of the controller thread.
        Accepts commands from external socket and relays them to ZMQAgent.
        Receives status updates from ZMQAgent and caches them.
        """
        while True:
            command = self.sock.recv_string()
            if not command:
                continue
            logging.info("Remote command: %s", command)
            tokens = command.strip().split(' ')
            self.sock.send_json(self.exec_command(tokens))
