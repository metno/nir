import os.path
import sqlite3
import syncer.exceptions
import logging


class Transaction(object):
    '''Add automatic transaction handling to database. The given connection
    must have isolation_level=None. See
    https://docs.python.org/3/library/sqlite3.html#sqlite3-controlling-transactions
    for information about why this is neccessary in this case.'''

    def __init__(self, database_connection):
        self.database_connection = database_connection

    def __enter__(self):
        self.cursor = self.database_connection.cursor()
        self.cursor.execute('begin')
        return self.cursor

    def __exit__(self, type, value, traceback):
        if type is None:
            self.cursor.execute('commit')
        else:
            self.cursor.execute('rollback')


class StateDatabase(object):
    '''Storage for temporary metadata that needs to live across program failures and terminations'''

    def __init__(self, db_file, create_if_missing=False):
        database_already_exists = os.path.exists(db_file) and db_file != ':memory:'
        if not database_already_exists and not create_if_missing:
            raise syncer.exceptions.MissingStateFile()
        self._connection = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
        if not database_already_exists:
            self._initialize_database()
        self._update()

    def _initialize_database(self):
        logging.debug('Creating new database file')
        with Transaction(self._connection) as c:
            c.execute('create table version (version int primary key, applied_at timestamp not null default current_timestamp)')
            c.execute('insert into version (version) values (0)')

    def _update(self):
        c = self._connection.execute('select max(version) from version')
        r = c.fetchone()
        if r:
            version = r[0]
        else:
            version = 0
        if version < 1:
            with Transaction(self._connection) as c:
                c.execute('create table loaded_data (productinstance text primary key, load_time timestamp not null default current_timestamp)')
                c.execute('create table pending_jobs (product_id text, reference_time timestamp not null, version int not null, productinstance_id text not null, force boolean not null default false)')
                c.execute('insert into version (version) values (1)')
        if version < 2:
            with Transaction(self._connection) as c:
                c.execute('create table last_data (model text not null, type text not null, datainstanceid text not null, reference_time timestamp not null, time_done timestamp not null default current_timestamp, constraint umt unique (model, type))')
                c.execute('insert into version (version) values (2)')

    def is_loaded(self, productinstance_id):
        c = self._connection.execute('select load_time from loaded_data where productinstance=?', (productinstance_id,))
        return bool(c.fetchone())

    def set_loaded(self, productinstance_id):
        with Transaction(self._connection) as c:
            c.execute('insert into loaded_data (productinstance) values (?)', (productinstance_id,))
            # at the same time, make sure database only contains relatively new data
            c.execute("delete from loaded_data where load_time<datetime('now', '-1 day')")

    def get_load_time(self, productinstance_id):
        c = self._connection.execute('select load_time from loaded_data where productinstance=?', (productinstance_id,))
        result = c.fetchone()
        if result:
            return result[0]
        return None

    DATA_AVAILABLE = 'data available'
    DATA_WDB_OK = 'data wdb ok'
    DATA_WDB2TS_OK = 'data wdb2ts ok'
    DATA_DONE = 'data ok'

    def set_last_incoming(self, model, type, datainstanceid, reference_time):
        with self._connection as c:
            c.execute('insert or replace into last_data (model, type, datainstanceid, reference_time) VALUES (?, ?, ?, ?)', (model, type, datainstanceid, reference_time.replace(tzinfo=None)))

    def get_last_incoming(self, model, type):
        c = self._connection.execute('select datainstanceid, reference_time, time_done from last_data where model=? and type=?', (model, type))
        result = c.fetchone()
        return result

    def pending_productinstances(self):
        ret = {}
        c1 = self._connection.cursor()
        c1.execute('select distinct product_id from pending_jobs')
        for r1 in c1:
            c2 = self._connection.cursor()
            c2.execute('select productinstance_id, force from pending_jobs where product_id=? order by reference_time desc, version desc limit 1', (r1[0],))
            for pid, force in c2:
                if pid not in ret:
                    ret[pid] = force
                elif force:
                    ret[pid] = True
        return ret

    def add_productinstance_to_be_processed(self, productinstance, force=False, even_if_previously_loaded=False):
        with Transaction(self._connection) as c:
            if even_if_previously_loaded:
                c.execute('delete from loaded_data where productinstance=?', (productinstance.id,))
            c.execute('insert into pending_jobs (product_id, reference_time, version, productinstance_id, force) values (?, ?, ?, ?, ?)',
                      (productinstance.product.id, productinstance.reference_time, productinstance.version, productinstance.id, force))

    def done(self, productinstance):
        with self._connection as c:
            c.execute('delete from pending_jobs where product_id=?', (productinstance.product.id,))
