"""
routine.py: part of YBS-broker that runs on routine servers.

Run python routine.py --help for program description and usage.
"""

import datetime
import argparse
import logging
import sqlite3
import json
import zmq
import re

DATABASE_FILENAME = '/tmp/dataqueue.db'
ZMQ_SOCKET = 'ipc:///tmp/dataqueue.zmq'

REPLY_NAK = u'NAK'
REPLY_OK = u'OK'

URI_REGEX = "[A-Za-z][A-Za-z0-9\+\.\-]*:([A-Za-z0-9\.\-_~:/\?#\[\]@!\$&'\(\)\*\+,;=]|%[A-Fa-f0-9]{2})+"


class InvalidDatasetException(Exception):
    pass


class ClientException(Exception):
    pass


class Database(object):
    """Interface to SQLite3."""

    def __init__(self):
        self.conn = sqlite3.connect(DATABASE_FILENAME)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA foreign_keys = on')
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        self.create_table_modeldata()
        self.create_table_filelist()

    def create_table_modeldata(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS "modeldata" (
            "id" INTEGER PRIMARY KEY NOT NULL,
            "model" TEXT NOT NULL,
            "date" TEXT NOT NULL,
            "term" INTEGER NOT NULL
            )'''
        )

    def create_table_filelist(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS "filelist" (
            "id" INTEGER PRIMARY KEY NOT NULL,
            "modeldata_id" INTEGER NOT NULL REFERENCES "modeldata" ("id") ON DELETE CASCADE,
            "uri" TEXT NOT NULL
            )'''
        )

    def put(self, model, date, term, files):
        self.cursor.execute('''INSERT INTO "modeldata" VALUES (NULL, ?, ?, ?)''', (model, date, term,))
        id = self.cursor.lastrowid
        for f in files:
            self.cursor.execute('''INSERT INTO "filelist" VALUES (NULL, ?, ?)''', (id, f['uri'],))
        self.conn.commit()

    def get(self):
        self.cursor.execute('''SELECT * FROM "modeldata" ORDER BY "id" ASC LIMIT 1''')
        data = self.cursor.fetchone()
        if not data:
            return
        self.cursor.execute('''SELECT "uri" FROM "filelist" WHERE "modeldata_id" = ?''', (data['id'],))
        data = dict(data)
        data['files'] = [dict(x) for x in self.cursor.fetchall()]
        return data

    def delete(self, id):
        self.cursor.execute('''DELETE FROM "modeldata" WHERE "id" = ?''', (id,))
        self.conn.commit()


class Broker(object):
    """The Broker class listens for client connections, and accepts datasets."""
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(ZMQ_SOCKET)
        self.uri_regex = re.compile(URI_REGEX)

    def send_nak(self, errmsg):
        self.socket.send_string(u"%s %s" % (REPLY_NAK, errmsg))

    def send_ok(self):
        self.socket.send_string(REPLY_OK)

    def load_and_validate(self, datastr):
        try:
            data = json.loads(datastr)
        except ValueError, e:
            raise InvalidDatasetException('Invalid JSON data: %s' % unicode(e))

        for key in ['model', 'date', 'term', 'files']:
            if not key in data:
                raise InvalidDatasetException('Missing required parameter "%s"' % key)
            if data[key] is None:
                raise InvalidDatasetException('Required parameter "%s" cannot be NULL' % key)

        try:
            datetime.datetime.strptime(data['date'], '%Y-%m-%d')
        except ValueError, e:
            raise InvalidDatasetException('Invalid date "%s": %s' % (data['date'], unicode(e)))

        try:
            term = int(data['term'])
            if term < 0 or term > 23:
                raise InvalidDatasetException('Term %d is not in required range 0-23' % term)

        except ValueError, e:
            raise InvalidDatasetException('Invalid term "%s": %s' % (data['term'], unicode(e)))

        for i, f in enumerate(data['files']):
            if not isinstance(f, dict):
                raise InvalidDatasetException('Unexpected type %s in files array index %d, expected hash' % (type(f), i))
            if 'uri' not in f:
                raise InvalidDatasetException('Expected "uri" key in files array index %d, found none' % i)
            if not self.uri_regex.match(f['uri']):
                raise InvalidDatasetException('Malformed URI "%s" in files array index %d, see RFC 3986')

        return data

    def recv(self):
        return self.socket.recv_string()


class Server(object):
    """The Server class mediates between the Database and Broker class. It
    ensures that datasets accepted through Broker are committed to Database,
    sent out to the REST service, and deleted when they are submitted.
    """

    def __init__(self):
        self.db = Database()
        self.broker = Broker()

    def post_dataset(self, data):
        logging.error("NOT IMPLEMENTED: post_dataset()")

    def post_all_queued(self):
        while True:
            data = self.db.get()
            if not data:
                return
            try:
                logging.info("Posting dataset: %s" % json.dumps(data))
                self.post_dataset(data)
                logging.info("Dataset successfully posted")
            except Exception, e:
                logging.error("Error posting dataset: %s" % unicode(e))
            else:
                self.db.delete(data['id'])
                logging.info("Dataset deleted from queue")

    def recv(self):
        try:
            s = self.broker.recv()
            logging.info("Received string: %s" % s)

            data = self.broker.load_and_validate(s)
            logging.info("Dataset passed validation")

            self.db.put(data['model'], data['date'], data['term'], data['files'])
            logging.info("Dataset stored in database")

            self.broker.send_ok()
            logging.info("Sent OK to client")

        except InvalidDatasetException, e:
            logging.warning("Discarding invalid dataset: %s" % unicode(e))
            self.broker.send_nak(unicode(e))

    def main(self):
        while True:
            self.post_all_queued()
            self.recv()


class Client(object):
    """The Client class submits datasets to an already running server instance."""
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(ZMQ_SOCKET)

    def _send_data(self, data):
        self.socket.send_string(data)

    def _tokenize(self, s):
        return [x.strip() for x in s.split()]

    def _recv_status(self):
        return self.socket.recv_string()

    def post_data(self, data):
        s = json.dumps(data)
        logging.info("Sending dataset: %s" % s)
        self._send_data(s)
        reply = self._recv_status()
        tokens = self._tokenize(reply)
        if len(tokens) == 0:
            raise ClientException("Got empty reply from server")
        if tokens[0] == REPLY_OK:
            return
        elif tokens[0] == REPLY_NAK:
            if len(tokens) == 1:
                raise ClientException("Got NAK from server, but no error message")
            raise ClientException(reply[len(tokens[0])+1:])
        raise ClientException("Unexpected reply from server: %s", reply)


if __name__ == '__main__':
    def server(args):
        app = Server()
        logging.info("Listening for connections on %s" % ZMQ_SOCKET)
        app.main()

    def client(args):
        app = Client()
        data = {
            "model": args.model,
            "date": args.date,
            "term": args.term,
            "files": [{ "uri": x } for x in args.files]
        }
        try:
            app.post_data(data)
        except ClientException, e:
            logging.error(unicode(e))

    def arg_term_type(x):
        x = int(x)
        if x < 0 or x > 23:
            raise argparse.ArgumentTypeError("Term must be in the range of 00-23")
        return x

    logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)

    desc = '''Message queue for posting data sets to YBS-broker.  
    The program uses a client/server architecture, use <subcommand> --help for detailed descriptions.'''
    parser = argparse.ArgumentParser(description=desc)
    subparsers = parser.add_subparsers()

    parser_server = subparsers.add_parser('server', help='start the message queue server', description='Starts the message queue server.')
    parser_server.set_defaults(func=server)

    parser_client = subparsers.add_parser('client', help='send dataset to message queue server', description='Sends a dataset to an already running message queue server.')
    parser_client.add_argument('--model', type=unicode, help='model name')
    parser_client.add_argument('--date', type=unicode, metavar='YYYY-MM-DD', help='model run date')
    parser_client.add_argument('--term', type=arg_term_type, metavar='00-23', help='model run term')
    parser_client.add_argument('files', metavar='uri', type=unicode, nargs='+', help='URI of files in this dataset')
    parser_client.set_defaults(func=client)

    args = parser.parse_args()
    try:
        args.func(args)
    except Exception, e:
        import sys, traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.critical("Uncaught exception! ABORT!")
        logging.critical("Exception was: %s", unicode(e))
        backtrace = traceback.extract_tb(exc_traceback)
        strings = traceback.format_list(backtrace)
        logging.debug('------------------------ traceback ------------------------')
        [logging.debug(x.strip()) for x in strings]
        logging.debug('---------------------- end traceback ----------------------')
