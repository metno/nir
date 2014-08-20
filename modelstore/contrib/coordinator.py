#
# Runs on WDB2TS machines, receives messages from WDB that data set has been imported
#

import imp
import zmq
import argparse
import operator
import json
import logging
import requests
import collections
import itertools

LOAD_THRESHOLD = 51

CMD_STATUS = 'status'
CMD_DATASET = 'dataset'
CMD_DATASETS = 'datasets'

REST_URI = 'http://localhost:8000/api/v1'

# Define these in your settings file
#SIBLINGS = []
#WDB_HOST = 'localhost'
#WDB_PORT = 5556
#PUB_HOST = '0.0.0.0'
#PUB_PORT = 5557

class REST:
    def __init__(self):
        pass

    def get_dataset_by_id(self, id):
        uri = "%s/dataset/%d/" % (REST_URI, id)
        req = requests.get(uri, headers={'content-type': 'application/json'})
        return json.loads(req.content)

class Sibling:
    def __init__(self, config, host, port):
        self.CSTR = 'tcp://%s:%d' % (host, port)
        self.config = config
        self.datasets = {}
        self.dataset = {}
        for model in self.config.MODELS:
            self.datasets[model] = []
            self.dataset[model] = None
        self.host = host
        self.socket = context.socket(zmq.SUB)
        self.socket.connect(self.CSTR)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, u'')

    def truncate_dataset_list(self, model):
        self.datasets[model] = []

    def has_dataset_id(self, id):
        return self.get_dataset_id(id) != None

    def get_dataset_id(self, id):
        for sets in self.datasets.itervalues():
            for dataset in sets:
                if dataset['id'] == id:
                    return dataset
        return None

    def add_dataset(self, dataset):
        if self.has_dataset_id(dataset['id']):
            return False
        dataset['id'] = dataset['id']
        if not dataset['model']['id'] in self.datasets:
            self.datasets[dataset['model']['id']] = []
        self.datasets[dataset['model']['id']] += [dataset]
        return True

class Server:
    def __init__(self, config, rest):
        self.poller = zmq.Poller()
        self.config = config
        self.rest = rest

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
                    logging.debug("Received message from WDB: %s" % message)
                    self.process_wdb(message)
                    self.broadcast_state()
                    self.reconfigure()
                else:
                    for sibling in self.siblings.itervalues():
                        if sock == sibling.socket:
                            logging.debug("Received message from sibling '%s': %s" % (sibling.host, message))
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
        #dataset_str  = "%s %s" % (CMD_DATASET, self.me.dataset if self.me.dataset else '')
        #self.publisher.send_string(dataset_str.strip())
        #logging.debug(dataset_str)
        for model in self.config.MODELS:
            datasets_str = "%s %s %s" % (CMD_DATASETS, model, ' '.join([str(x['id']) for x in self.me.datasets[model]]))
            self.publisher.send_string(datasets_str.strip())
            logging.debug(datasets_str)

    def process_wdb(self, message):
        dataset = self.rest.get_dataset_by_id(int(message))
        if self.me.add_dataset(dataset):
            logging.info("Added dataset %d to list of available datasets." % dataset['id'])

    def process_sibling(self, sibling, message):
        tokens = message.split(" ")
        if tokens[0] == CMD_DATASET:
            if len(tokens) == 2:
                sibling.dataset[tokens[1]] = None
            else:
                sibling.dataset[tokens[1]] = int(tokens[2])
        elif tokens[0] == CMD_DATASETS:
            sets = []
            for id in tokens[2:]:
                dataset = sibling.get_dataset_id(int(id))
                if not dataset:
                    dataset = self.rest.get_dataset_by_id(int(id))
                sets += [dataset]
            sibling.truncate_dataset_list(tokens[1])
            [sibling.add_dataset(dataset) for dataset in sets]
            self.reconfigure()
        elif tokens[0] == CMD_STATUS:
            self.broadcast_state()

    def get_availability(self, model):
        allsets = [y['id'] for y in list(itertools.chain(*[x.datasets[model] for x in self.siblings.itervalues()]))]
        sortedsets = sorted(set(allsets), reverse=True)
        counts = collections.Counter(allsets)
        prospect = None
        pop = dict(counts)
        for dataset in sortedsets:
            hosts = pop[dataset]
            percent = (float(hosts) / len(self.siblings)) * 100
            if not prospect and percent >= LOAD_THRESHOLD:
                prospect = (dataset, hosts,)
        return sortedsets, pop, prospect

    def print_availability(self, model, sortedsets, pop):
        for dataset in sortedsets:
            hosts = pop[dataset]
            percent = (float(hosts) / len(self.siblings)) * 100
            current = "[current]" if self.me.dataset[model] == dataset else ""
            logging.info("  %6d: %2d hosts [%3d%%] %s" % (dataset, hosts, percent, current))

    def reconfigure(self):
        for model in self.config.MODELS:
            if len(self.me.datasets[model]) == 0:
                logging.warning("No datasets loaded for model %s" % model)
                continue

            sortedsets, pop, prospect = self.get_availability(model)

            if not prospect:
                dataset = sortedsets[0]
                hosts = pop[dataset]
                percent = (float(hosts) / len(self.siblings)) * 100
                logging.info("%s: %d/%d hosts has new dataset %d, %d%% is still below threshold of %d%%" % (model, hosts, len(self.siblings), dataset, percent, LOAD_THRESHOLD))
                return

            dataset, hosts = prospect
            percent = (float(hosts) / len(self.siblings)) * 100

            if dataset == self.me.dataset[model]:
                return

            if not self.me.has_dataset_id(dataset):
                logging.warning("I don't have the most popular %s dataset and I should be disabled!" % model)
                return

            logging.info("%s: %d/%d hosts has new dataset %d, %d%% is above threshold of %d%%: loading dataset!" % (model, hosts, len(self.siblings), dataset, percent, LOAD_THRESHOLD))
            self.load_dataset_id(model, dataset)

            logging.info("===== %s availability =====" % model)
            self.print_availability(model, sortedsets, pop)
            logging.info("===== end of availability report =====")

    def load_dataset_id(self, model, id):
        logging.info("Setting currently loaded %s dataset to %d" % (model, id))
        self.me.dataset[model] = id
        self.broadcast_state()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s', level=logging.DEBUG)
    context = zmq.Context()

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='python settings module name', required=True)

    args = parser.parse_args()

    config = imp.load_source('config', args.config)
    rest = REST()

    server = Server(config, rest)
    server.main()
