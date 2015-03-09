"""
Modelstatus ZMQ module.

Modelstatus runs a ZMQ publisher that notifies clients about interesting data sets.
"""

import zmq
import errno
import logging

#
# Version for the ZeroMQ **message format**.
# Change the version when you make changes to the data returned from the
# function `message_from_resource'.
#
# Follows Semantic Versioning 2.0.0: http://semver.org/spec/v2.0.0.html
#
MESSAGE_PROTOCOL_VERSION = [1, 1, 0]


class ZMQPublisher(object):
    def __init__(self, addr):
        self.context = zmq.Context()
        self.sock = self.context.socket(zmq.PUB)
        self.sock.bind(addr)

    def publish_resource(self, resource):
        msg = self.message_from_resource(resource)
        while True:
            try:
                self.sock.send_json(msg)
                break
            except zmq.ZMQError, e:
                if e.errno == errno.EINTR:
                    logging.warning("Interrupted while transmitting ZeroMQ message, retrying...")
                    continue
                else:
                    raise
        logging.info("Published ZeroMQ message: %s" % msg)

    def message_from_resource(self, resource):
        return {
            'version': MESSAGE_PROTOCOL_VERSION,
            'type': 'resource',
            'resource': unicode(resource.__table__),
            'id': resource.id,
        }
