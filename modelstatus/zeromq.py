"""
Modelstatus ZMQ module.

Modelstatus runs a ZMQ publisher that notifies clients about interesting data sets.
"""

import zmq
import logging


class ZMQPublisher(object):
    def __init__(self, addr):
        self.context = zmq.Context.instance()
        self.sock = self.context.socket(zmq.PUB)
        self.sock.bind(addr)

    def publish_resource(self, resource):
        msg = self.message_from_resource(resource)
        self.sock.send_string(msg)
        logging.info("Published ZeroMQ message: %s" % msg)

    def message_from_resource(self, resource):
        return unicode(u"%s %d" % (resource.__table__, resource.id))
