import sqlite3
import json
import zmq

DATABASE_FILENAME = '/tmp/dataqueue.db'
ZMQ_SOCKET = 'ipc:///tmp/dataqueue.zmq'

REPLY_NAK = u'NAK'
REPLY_OK = u'OK'

class Database(object):
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

class App(object):
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

if __name__ == '__main__':
    app = App()
    app.main()
