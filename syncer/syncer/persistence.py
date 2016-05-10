import os.path
import sqlite3
import syncer.exceptions
import logging


class StateDatabase(object):
    '''Storage for temporary metadata that needs to live across program failures and terminations'''

    def __init__(self, db_file, create_if_missing=False):
        if not os.path.exists(db_file):
            if create_if_missing:
                self._connection = StateDatabase.create_database(db_file)
            else:
                raise syncer.exceptions.MissingStateFile()
        else:
            self._connection = sqlite3.connect(db_file)
        self._update()

    @staticmethod
    def create_database(db_file):
        logging.debug('Creating new database file at ' + db_file)
        connection = sqlite3.connect(db_file)
        c = connection.cursor()
        c.execute('create table version (version int primary key, applied_at timestamp not null default current_timestamp)')
        c.execute('insert into version (version) values (0)')
        connection.commit()
        return connection

    def _update(self):
        c = self._connection.cursor()
        c.execute('select max(version) from version')
        version = 0
        r = c.fetchone()
        version = r[0]
        if version < 1:
            c.execute('create table loaded_data (productinstance text primary key, load_time timestamp not null default current_timestamp)')
            c.execute('create table pending_jobs (product_id text, reference_time timestamp not null, version int not null, productinstance_id text not null)')
            c.execute('insert into version (version) values (1)')
        self._connection.commit()

    def is_loaded(self, productinstance_id):
        c = self._connection.cursor()
        c.execute('select load_time from loaded_data where productinstance=?', (productinstance_id,))
        return bool(c.fetchone())

    def set_loaded(self, productinstance_id):
        c = self._connection.cursor()
        c.execute('insert into loaded_data (productinstance) values (?)', (productinstance_id,))
        # at the same time, make sure database only contains relatively new data
        c.execute("delete from loaded_data where load_time<datetime('now', '-1 day')")

        self._connection.commit()

    def pending_productinstances(self):
        c1 = self._connection.cursor()
        c1.execute('select distinct product_id from pending_jobs')
        for r1 in c1:
            c2 = self._connection.cursor()
            c2.execute('select productinstance_id from pending_jobs where product_id=? order by reference_time desc, version desc', (r1[0],))
            for r2 in c2:
                yield r2[0]

    def add_productinstance_to_be_processed(self, productinstance):
        c = self._connection.cursor()
        c.execute('insert into pending_jobs (product_id, reference_time, version, productinstance_id) values (?, ?, ?, ?)',
                  (productinstance.product.id, productinstance.reference_time, productinstance.version, productinstance.id))
        self._connection.commit()

    def done(self, productinstance):
        # del self.pending[productinstance.product.id]
        c = self._connection.cursor()
        c.execute('delete from pending_jobs where product_id=?', (productinstance.product.id,))
        self._connection.commit()
