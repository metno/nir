#
# Runs on WDB machines, receives messages from broker with new data sets
#

import zmq
import time
import json
import logging

SUB_SOCKET = 'tcp://localhost:5555'
PUB_SOCKET = 'tcp://*:5556'

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s', level=logging.DEBUG)
    context = zmq.Context()

    subscriber = context.socket(zmq.SUB)
    subscriber.connect(SUB_SOCKET)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, u'')
    logging.info("Ready to receive messages")

    publisher = context.socket(zmq.PUB)
    publisher.bind(PUB_SOCKET)

    while True:
        s = subscriber.recv_string()
        dataset = json.loads(s)
        logging.info("Upstream has new dataset: %s %s term %02d with %d files" % (dataset['model'], dataset['date'], dataset['term'], len(dataset['files'])))
        for f in dataset['files']:
            logging.info("Loading file '%s'" % f)
            cmd = "/usr/bin/netcdfLoad -c/etc/netcdfLoad/arome.netcdfload.xml --loadPlaceDefinition --dataprovider=%(model)s --placename=%(model)s_grid %(file)s" % {
                    "model": dataset["model"],
                    "file": f
                    }
            logging.debug(cmd)

        logging.info("Loading complete")
        publisher.send_string("%s" % dataset['id'])
        logging.info("Published finish message")
