"""
Modelstatus ZMQ module.

Modelstatus runs a ZMQ publisher that notifies clients about interesting data sets.
"""

import zmq
import errno
import logging


class ZMQPublisher(object):
    def __init__(self, addr):
        self.context = zmq.Context()
        self.sock = self.context.socket(zmq.PUB)
        self.sock.bind(addr)

    def publish_resource(self, resource):
        msg = self.message_from_resource(resource)
        while True:
            try:
                self.sock.send_string(msg)
                break
            except zmq.ZMQError, e:
                if e.errno == errno.EINTR:
                    logging.warning("Interrupted while transmitting ZeroMQ message, retrying...")
                    continue
                else:
                    raise
        logging.info("Published ZeroMQ message: %s" % msg)

    def message_from_resource(self, resource):
        return unicode(u"%s %d" % (resource.__table__, resource.id))
