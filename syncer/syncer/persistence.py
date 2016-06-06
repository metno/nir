import os.path
import sqlite3
import syncer.exceptions
import logging


class StateDatabase(object):
    '''Storage for temporary metadata that needs to live across program failures and terminations'''

    def __init__(self, db_file, create_if_missing=False):
        database_already_exists = os.path.exists(db_file) and db_file != ':memory:'
        if not database_already_exists and not create_if_missing:
            raise syncer.exceptions.MissingStateFile()
        self._connection = sqlite3.connect(db_file)
        if not database_already_exists:
            self._initialize_database()
        self._update()

    def _initialize_database(self):
        logging.debug('Creating new database file')
        with self._connection as c:
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
            with self._connection as c:
                c.execute('create table loaded_data (productinstance text primary key, load_time timestamp not null default current_timestamp)')
                c.execute('create table pending_jobs (product_id text, reference_time timestamp not null, version int not null, productinstance_id text not null, force boolean not null default false)')
                c.execute('insert into version (version) values (1)')
#         if version < 2:
#             with self._connection as c:
#                 c.execute('alter table pending_jobs rename to pending_jobs_old')
#                 c.execute('create table pending_jobs (product_id text, reference_time timestamp not null, version int not null, productinstance_id text not null, force boolean not null default false, pending_since timestamp not null default current_timestamp)')
#                 c.execute('insert into pending_jobs (select product_id, reference_time, version, productinstance_id, force from pending_jobs_old)')
#                 c.execute('drop table pending_jobs_old')
#                 c.execute('insert into version (version) values (2)')

    def is_loaded(self, productinstance_id):
        c = self._connection.execute('select load_time from loaded_data where productinstance=?', (productinstance_id,))
        return bool(c.fetchone())

    def set_loaded(self, productinstance_id):
        with self._connection as c:
            c.execute('insert into loaded_data (productinstance) values (?)', (productinstance_id,))
            # at the same time, make sure database only contains relatively new data
            c.execute("delete from loaded_data where load_time<datetime('now', '-1 day')")

    def get_load_time(self, productinstance_id):
        c = self._connection.execute('select load_time from loaded_data where productinstance=?', (productinstance_id,))
        result = c.fetchone()
        if result:
            return result[0]
        return None

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
                    ret[ret[pid]] = True
        return ret

    def add_productinstance_to_be_processed(self, productinstance, force=False):
        with self._connection as c:
            if force:
                c.execute('delete from loaded_data where productinstance=?', (productinstance.id,))
            c.execute('insert into pending_jobs (product_id, reference_time, version, productinstance_id, force) values (?, ?, ?, ?, ?)',
                      (productinstance.product.id, productinstance.reference_time, productinstance.version, productinstance.id, force))

    def done(self, productinstance):
        with self._connection as c:
            c.execute('delete from pending_jobs where product_id=?', (productinstance.product.id,))
