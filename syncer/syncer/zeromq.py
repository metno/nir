"""
Syncer ZMQ module.

Syncer runs a ZMQ subscriber that listens to events from Modelstatus.
"""

import zmq
import logging

import syncer.exceptions


class ZMQBase(object):

    STATUS_OK = 0
    STATUS_FAIL = 1
    STATUS_INVALID = 2

    def make_reply(self, code, data):
        return {
            'status': code,
            'data': data
        }


class ZMQEvent(ZMQBase):
    required_fields = []

    def __init__(self, **kwargs):
        [setattr(self, key, value) for key, value in kwargs.iteritems()]

        for member in self.required_fields + ['version']:
            if not hasattr(self, member):
                raise syncer.exceptions.ZMQEventIncomplete("ZMQEvent is missing the '%s' field" % member)

        # validate version
        try:
            assert len(self.version) == 3
            self.version = [int(x) for x in self.version]
            assert self.version[0] == 1
            assert self.version[1] >= 1
            assert self.version[2] >= 0
        except:
            raise syncer.exceptions.ZMQEventUnsupportedVersion("ZMQEvent is of unsupported version: %s" % self.version)

        self.validate()

    def validate(self):
        pass

    @staticmethod
    def factory(**kwargs):
        if 'type' not in kwargs:
            raise syncer.exceptions.ZMQEventIncomplete("ZMQEvent has undefined type!")
        if kwargs['type'] == 'resource':
            return ZMQResourceEvent(**kwargs)

    def __repr__(self):
        return "ZeroMQ event type=%s version=%s %s" % (
            self.type,
            '.'.join([str(x) for x in self.version]),
            ' '.join(["%s=%s" % (key, getattr(self, key)) for key in self.required_fields])
        )


class ZMQResourceEvent(ZMQEvent):
    required_fields = ['id', 'resource']

    def validate(self):
        # validate id
        try:
            assert self.id == int(self.id)
        except:
            raise syncer.exceptions.ZMQEventBadId("ZMQEvent has bad ID: %s" % str(self.id))

        # validate resource
        try:
            self.resource = unicode(self.resource)
            assert len(self.resource) > 0
        except:
            raise syncer.exceptions.ZMQEventBadResource("ZMQEvent has bad resource: %s" % str(self.resource))


class ZMQSubscriber(ZMQBase):
    def __init__(self, addr, tcp_keepalive_interval, tcp_keepalive_count):
        self.context = zmq.Context()
        self.sock = self.context.socket(zmq.SUB)
        self.sock.setsockopt(zmq.TCP_KEEPALIVE, 1)                             # enable TCP keepalive
        self.sock.setsockopt(zmq.TCP_KEEPALIVE_IDLE, tcp_keepalive_interval)   # keepalive packet sent each N seconds
        self.sock.setsockopt(zmq.TCP_KEEPALIVE_INTVL, tcp_keepalive_interval)  # keepalive packet sent each N seconds
        self.sock.setsockopt(zmq.TCP_KEEPALIVE_CNT, tcp_keepalive_count)       # number of missed packets to mark connection as dead
        self.sock.connect(addr)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, u'')

    def recv(self):
        """
        Receive incoming messages, and return them.
        """
        try:
            msg = self.sock.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, e:
            logging.debug("%s in ZMQSubscriber.recv(): %s" % (type(e), unicode(e)))
            msg = None
        return msg

    def get_event(self):
        """
        Check if there are valid resource update events in the ZeroMQ pipeline.
        Returns a ZMQEvent object.
        """
        msg = self.recv()
        try:
            event = ZMQEvent.factory(**msg)
            return event
        except TypeError, e:
            logging.warning("Discarding spurious event from ZeroMQ publisher: %s" % unicode(msg))
            logging.warning("Python exception: %s" % unicode(e))
        except syncer.exceptions.ZMQEventException, e:
            logging.warning("Discarding incompatible event from ZeroMQ publisher: %s" % unicode(msg))
            logging.warning("ZMQEvent exception: %s" % unicode(e))
        return None


class ZMQAgent(ZMQBase):
    """
    Receives commands from a ZMQController.
    """
    def __init__(self):
        self.context = zmq.Context()
        self.init_req('tcp://127.0.0.1:59900')
        self.init_rep('tcp://127.0.0.1:59901')

    def init_req(self, addr):
        """
        Initialize a ZMQ IPC socket which sends status updates to ZMQController.
        """
        self.req = self.context.socket(zmq.REQ)
        self.req.setsockopt(zmq.SNDTIMEO, 2000)
        self.req.bind(addr)

    def init_rep(self, addr):
        """
        Initialize a ZMQ IPC socket which receives commands from ZMQController.
        """
        self.rep = self.context.socket(zmq.REP)
        self.rep.setsockopt(zmq.SNDTIMEO, 2000)
        self.rep.bind(addr)

    def sync_status(self, data):
        """
        Synchronize status with ZMQController.
        """
        self.req.send_json(data)
        self.req.recv()

    def get_command(self):
        return self.rep.recv_json()

    def send_command_response(self, status, data):
        return self.rep.send_json(self.make_reply(status, data))


class ZMQController(ZMQBase):
    """
    API to Syncer providing status queries and command issuing.
    """
    def __init__(self, addr):
        self.context = zmq.Context()
        self.init_rep('tcp://127.0.0.1:59900')
        self.init_req('tcp://127.0.0.1:59901')
        self.init_sock(addr)
        self.poller = zmq.Poller()
        self.poller.register(self.sock, zmq.POLLIN)
        self.poller.register(self.rep, zmq.POLLIN)
        self.status = {
            'models': []
        }

    def init_req(self, addr):
        """
        Initialize a ZMQ IPC socket which requests that commands are run on ZMQAgent.
        """
        self.req = self.context.socket(zmq.REQ)
        self.req.connect(addr)

    def init_rep(self, addr):
        """
        Initialize a ZMQ IPC socket which receives status updates from ZMQAgent.
        """
        self.rep = self.context.socket(zmq.REP)
        self.rep.connect(addr)

    def init_sock(self, addr):
        """
        Initialize the external facing socket, listening for commands from the admin.
        """
        self.sock = self.context.socket(zmq.REP)
        self.sock.bind(addr)

    def run_hello(self):
        """
        Hello World command.
        """
        return self.make_reply(self.STATUS_OK, ['Hello, world!'])

    def run_status(self):
        """
        Return information about model status.
        """
        return self.make_reply(self.STATUS_OK, self.status['models'])

    def run_load(self, model_run_id):
        """
        (Re)-load a model run into WDB.
        """
        model_run_id = int(model_run_id)
        return self.exec_syncer({'command': 'load', 'model_run_id': model_run_id})

    def exec_syncer(self, command):
        """
        Send a command to Syncer.
        """
        self.req.send_json(command)
        return self.req.recv_json()

    def exec_command(self, tokens):
        try:
            if 'command' not in tokens:
                raise Exception('Missing command')
            if tokens['command'] == 'hello':
                return self.run_hello()
            if tokens['command'] == 'status':
                return self.run_status()
            if tokens['command'] == 'load':
                return self.run_load(tokens['model_run_id'])
            raise Exception("Invalid command '%s'" % tokens['command'])
        except Exception, e:
            return self.make_reply(self.STATUS_INVALID, [unicode(e)])

    def run(self):
        """
        Main loop of the controller thread.
        Accepts commands from external socket and relays them to ZMQAgent.
        Receives status updates from ZMQAgent and caches them.
        """
        while True:
            events = dict(self.poller.poll())
            if self.sock in events:
                try:
                    tokens = self.sock.recv_json()
                    self.sock.send_json(self.exec_command(tokens))
                except ValueError:
                    logging.warning("Some perpetrator is sending non-JSON data to the ZeroMQ control socket, message ignored.")
                    self.sock.send_string(u'go away')
            if self.rep in events:
                self.status = self.rep.recv_json()
                self.rep.send(b'')
