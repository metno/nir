"""
Syncer ZMQ module.

Syncer runs a ZMQ subscriber that listens to events from Modelstatus.
"""

import zmq


class ZMQEvent(object):
    def __init__(self, **kwargs):
        [setattr(self, key, value) for key, value in kwargs.iteritems()]

    def __repr__(self):
        return "ZeroMQ event resource=%s id=%d" % (self.resource, self.id)


class ZMQSubscriber(object):
    def __init__(self, addr):
        self.context = zmq.Context.instance()
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
