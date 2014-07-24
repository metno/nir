"""
This module is the part of YBS-broker that runs on routine servers.

See the documentation for the different classes.
"""

import argparse
import sqlite3
import json
import zmq

DATABASE_FILENAME = '/tmp/dataqueue.db'
ZMQ_SOCKET = 'ipc:///tmp/dataqueue.zmq'

REPLY_NAK = u'NAK'
REPLY_OK = u'OK'

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
    """The Broker class listens for client connections, and accepts data sets."""
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(ZMQ_SOCKET)

    def send_nak(self, errmsg):
        self.socket.send_string(u"%s %s" % (REPLY_NAK, errmsg))

    def send_ok(self):
        self.socket.send_string(REPLY_OK)

    def recv(self):
        """Expects a JSON string with the following data:
        {
            "model": "arome_metcoop_2500",
            "date": "2014-07-24",
            "term": 6,
            "files": [
                {
                    "uri": "opdata:///arome2_5_main/AROME_MetCoOp_06_DEF.nc"
                }
            ]
        }
        """
        s = self.socket.recv_string()
        data = json.loads(s)
        return data


class Server(object):
    """The Server class mediates between the Database and Broker class. It
    ensures that data sets accepted through Broker are committed to Database,
    sent out to the REST service, and deleted when they are submitted.
    """

    def __init__(self):
        self.db = Database()
        self.broker = Broker()

    def post_dataset(self, data):
        print 'Post dataset', json.dumps(data)

    def post_all_queued(self):
        while True:
            data = self.db.get()
            if not data:
                return
            try:
                self.post_dataset(data)
            except:
                print 'Error posting dataset'
            else:
                self.db.delete(data['id'])

    def recv(self):
        try:
            data = self.broker.recv()
            print 'Received data', json.dumps(data)
            self.db.put(data['model'], data['date'], data['term'], data['files'])
            self.broker.send_ok()
        except Exception, e:
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
        s = json.dumps(data)
        self.socket.send_string(s)

    def _tokenize(self, s):
        return [x.strip() for x in s.split()]

    def _recv_status(self):
        return self.socket.recv_string()

    def post_data(self, data):
        self._send_data(data)
        reply = self._recv_status()
        tokens = self._tokenize(reply)
        if len(tokens) == 0:
            raise Exception("Got empty reply from server")
        if tokens[0] == REPLY_OK:
            return
        elif tokens[0] == REPLY_NAK:
            if len(tokens) == 1:
                raise Exception("Got NAK from server, but no error message")
            raise Exception(reply[len(tokens[0])+1:])
        raise Exception("Unexpected reply from server: %s", reply)


if __name__ == '__main__':
    def server(args):
        app = Server()
        app.main()

    def client(args):
        app = Client()
        data = {
            "model": args.model,
            "date": args.date,
            "term": args.term,
            "files": [{ "uri": x } for x in args.files]
        }
        app.post_data(data)

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_server = subparsers.add_parser('server', help='start the server')
    parser_server.set_defaults(func=server)

    parser_client = subparsers.add_parser('client', help='send dataset to server')
    parser_client.add_argument('--model', type=unicode, help='model name, e.g. arome_metcoop_2500')
    parser_client.add_argument('--date', type=unicode, help='model run date in YYYY-MM-DD format')
    parser_client.add_argument('--term', type=int, help='model run term')
    parser_client.add_argument('files', metavar='uri', type=unicode, nargs='+', help='URI of files in this dataset')
    parser_client.set_defaults(func=client)

    args = parser.parse_args()
    args.func(args)
