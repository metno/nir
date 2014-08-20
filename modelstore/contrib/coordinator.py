#
# Runs on WDB2TS machines, receives messages from WDB that data set has been imported
#

import imp
import zmq
import argparse
import operator
import json
import logging
import collections
import itertools

LOAD_THRESHOLD = 51

CMD_STATUS = 'status'
CMD_DATASET = 'dataset'
CMD_DATASETS = 'datasets'

# Define these in your settings file
#SIBLINGS = []
#WDB_HOST = 'localhost'
#WDB_PORT = 5556
#PUB_HOST = '0.0.0.0'
#PUB_PORT = 5557

class Sibling:
    def __init__(self, config, host, port):
        self.CSTR = 'tcp://%s:%d' % (host, port)
        self.config = config
        self.datasets = []
        self.dataset = None
        self.host = host
        self.socket = context.socket(zmq.SUB)
        self.socket.connect(self.CSTR)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, u'')

class Server:
    def __init__(self, config):
        self.poller = zmq.Poller()
        self.config = config

        WDB_CSTR = 'tcp://%s:%d' % (self.config.WDB_HOST, self.config.WDB_PORT)
        self.wdb_subscriber = context.socket(zmq.SUB)
        self.wdb_subscriber.connect(WDB_CSTR)
        self.wdb_subscriber.setsockopt_string(zmq.SUBSCRIBE, u'')
        self.poller.register(self.wdb_subscriber, zmq.POLLIN)
        logging.info("Subscribed to WDB messages on %s" % WDB_CSTR)

        self.siblings = {}
        for num, host in enumerate(self.config.SIBLINGS):
            self.siblings[host] = Sibling(self.config, host, self.config.PUB_PORT)
            self.poller.register(self.siblings[host].socket, zmq.POLLIN)
            logging.info("Subscribed to sibling #%d on %s" % (num, self.siblings[host].CSTR))

        PUB_CSTR = 'tcp://%s:%d' % (self.config.PUB_HOST, self.config.PUB_PORT)
        logging.info("Publishing messages to WDB messages on %s" % PUB_CSTR)
        self.publisher = context.socket(zmq.PUB)
        self.publisher.bind(PUB_CSTR)

    def main(self):
        logging.info("Ready to process messages")
        import time
        time.sleep(1)
        self.catch_up()

        while True:
            try:
                socks = dict(self.poller.poll())
            except KeyboardInterrupt:
                break

            for sock in socks:
                message = sock.recv_string()
                if sock == self.wdb_subscriber:
                    logging.info("Received message from WDB: %s" % message)
                    self.process_wdb(message)
                    self.broadcast_state()
                    self.reconfigure()
                else:
                    for sibling in self.siblings.itervalues():
                        if sock == sibling.socket:
                            logging.info("Received message from sibling '%s': %s" % (sibling.host, message))
                            self.process_sibling(sibling, message)
            #self.broadcast_state()

    @property
    def me(self):
        return self.siblings[self.config.PUB_HOST]

    def catch_up(self):
        logging.info("Asking siblings for their current dataset")
        self.broadcast_state()
        self.publisher.send_string(CMD_STATUS)

    def broadcast_state(self):
        logging.info("Broadcasting my dataset status")
        dataset_str  = "%s %s" % (CMD_DATASET, self.me.dataset if self.me.dataset else '')
        datasets_str = "%s %s" % (CMD_DATASETS, ' '.join([str(x) for x in self.me.datasets]))
        self.publisher.send_string(dataset_str.strip())
        self.publisher.send_string(datasets_str.strip())
        logging.debug(dataset_str)
        logging.debug(datasets_str)

    def process_wdb(self, message):
        self.me.datasets += [int(message)]
        self.me.datasets = list(set(self.me.datasets))
        logging.info("Added dataset %d to list of available datasets." % int(message))

    def process_sibling(self, sibling, message):
        tokens = message.split(" ")
        if tokens[0] == CMD_DATASET:
            if len(tokens) == 1:
                sibling.dataset = None
            else:
                sibling.dataset = int(tokens[1])
        elif tokens[0] == CMD_DATASETS:
            sibling.datasets = [int(x) for x in tokens[1:]]
            self.reconfigure()
        elif tokens[0] == CMD_STATUS:
            self.broadcast_state()

    def reconfigure(self):
        logging.info("Checking dataset availability")
        if len(self.me.datasets) == 0:
            logging.warning("No datasets available locally")
            return

        counts = collections.Counter(list(itertools.chain(*[x.datasets for x in self.siblings.itervalues()])))

        for dataset, hosts in counts.most_common():
            percent = (float(hosts) / len(self.siblings)) * 100
            current = "[current]" if self.me.dataset == dataset else ""
            logging.info("  %6d: %2d hosts [%3d%%] %s" % (dataset, hosts, percent, current))

        dataset, hosts = counts.most_common(1)[0]
        percent = (float(hosts) / len(self.siblings)) * 100

        if dataset == self.me.dataset:
            return

        if dataset not in self.me.datasets:
            logging.warning("I'm using an outdated dataset and should be disabled!")
            return

        if percent < LOAD_THRESHOLD:
            logging.info("Not enough hosts with the most popular dataset, below threshold of %d%%" % LOAD_THRESHOLD)
            return

        logging.info("Going to load a new dataset since it is available on %d%% of hosts (threshold %d%%)" % (percent, LOAD_THRESHOLD))
        self.load_dataset_id(dataset)

    def load_dataset_id(self, id):
        logging.info("Setting currently loaded dataset to %d" % id)
        self.me.dataset = id
        self.broadcast_state()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s', level=logging.DEBUG)
    context = zmq.Context()

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='python settings module name', required=True)

    args = parser.parse_args()

    config = imp.load_source('config', args.config)

    server = Server(config)
    server.main()
