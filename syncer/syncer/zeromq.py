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

    def tokenize(self, s):
        return s.strip().split(' ')


class ZMQEvent(ZMQBase):
    def __init__(self, **kwargs):
        [setattr(self, key, value) for key, value in kwargs.iteritems()]
        self.validate()

    def validate(self):
        for member in ['id', 'resource', 'version']:
            if not hasattr(self, member):
                raise syncer.exceptions.ZMQEventIncomplete("ZMQEvent is missing the '%s' field" % member)

        # validate version
        try:
            assert len(self.version) == 3
            self.version = [int(x) for x in self.version]
            assert self.version[0] == 1
            assert self.version[1] >= 0
            assert self.version[2] >= 0
        except:
            raise syncer.exceptions.ZMQEventUnsupportedVersion("ZMQEvent is of unsupported version: %s" % self.version)

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

    def __repr__(self):
        return "ZeroMQ event version=%s resource=%s id=%d" % ('.'.join([str(x) for x in self.version]), self.resource, self.id)


class ZMQSubscriber(ZMQBase):
    def __init__(self, addr):
        self.context = zmq.Context()
        self.sock = self.context.socket(zmq.SUB)
        self.sock.connect(addr)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, u'')

    def recv(self):
        """
        Receive incoming messages, and return them.
        """
        try:
            msg = self.sock.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError:
            msg = None
        return msg

    def get_event(self):
        """
        Check if there are valid resource update events in the ZeroMQ pipeline.
        Returns a ZMQEvent object.
        """
        msg = self.recv()
        try:
            event = ZMQEvent(**msg)
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
        return self.tokenize(self.rep.recv_string())

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
        return self.exec_syncer('load %d' % model_run_id)

    def exec_syncer(self, command):
        """
        Send a command to Syncer.
        """
        self.req.send_string(unicode(command))
        return self.req.recv_json()

    def exec_command(self, tokens):
        try:
            if len(tokens) == 0:
                raise Exception('Empty command list')
            if tokens[0] == 'hello':
                return self.run_hello()
            if tokens[0] == 'status':
                return self.run_status()
            if tokens[0] == 'load':
                return self.run_load(tokens[1])
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
            events = dict(self.poller.poll())
            if self.sock in events:
                command = self.sock.recv_string()
                logging.info("Received remote command: %s", command)
                tokens = self.tokenize(command)
                self.sock.send_json(self.exec_command(tokens))
            if self.rep in events:
                self.status = self.rep.recv_json()
                self.rep.send(b'')
