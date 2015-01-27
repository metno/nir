import sqlalchemy.inspection
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.engine
import sqlalchemy.event
import datetime
import dateutil.tz

Base = sqlalchemy.ext.declarative.declarative_base()

class SerializeBase(object):
    __serializable__ = []

    def serialize(self):
        serialized = {}
        for key in self.__serializable__:
            func_name = 'serialize_' + key
            func = getattr(self, func_name, None)
            serialized[key] = getattr(self, key)
            if callable(func):
                serialized[key] = func(serialized[key])
            elif hasattr(serialized[key], 'serialize'):
                serialized[key] = serialized[key].serialize()
        return serialized
    
    def _serialize_datetime(self, value):
        """
        The database should save everything in UTC, but not all databases
        support time zones. This function ensures that a time zone-aware object
        is returned.
        """
        utc_time = value.replace(tzinfo=dateutil.tz.tzutc())
        return utc_time.isoformat()    

    def __repr__(self):
        return "<%s id=%s>" % (self.__tablename__, self.id)


class ModelRun(Base, SerializeBase):
    __tablename__ = 'model_run'
    __serializable__ = ['id', 'reference_time', 'data_provider', 'created_date', 'version', 'data']

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    reference_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True),
                                       nullable=False)
    data_provider = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    version = sqlalchemy.Column(sqlalchemy.Integer)
    created_date = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True),
                                     nullable=False,
                                     default=datetime.datetime.utcnow)
    __table_args__ = (sqlalchemy.UniqueConstraint('data_provider', 'reference_time', 'version'),)

    def serialize_reference_time(self, value):
        return self._serialize_datetime(value)

    def serialize_created_date(self, value):
        return self._serialize_datetime(value)

    def serialize_data(self, value):
        return [x.serialize() for x in value]


class Data(Base, SerializeBase):
    __tablename__ = 'data'
    __serializable__ = ['id', 'href', 'format', 'model_run_id', 'created_time']

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    href = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    format = sqlalchemy.Column(sqlalchemy.String(64), nullable=False)
    model_run_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('model_run.id'), nullable=False)
    model_run = sqlalchemy.orm.relationship(ModelRun, backref='data')
    created_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True),
                                     nullable=False,
                                     default=datetime.datetime.utcnow)

    def serialize_created_time(self, value):
        return self._serialize_datetime(value)


def get_sqlite_memory_session():
    return get_database_session('sqlite://')

def get_database_session(connection_uri):
    """
    Create the session from an uri. If the engine created use the sqlite dialect
    then setup to use specific sqlite configuration on each connection. 
    """
    engine = sqlalchemy.engine.create_engine(connection_uri)
    if engine.name == "sqlite":
        sqlalchemy.event.listen(sqlalchemy.engine.Engine, "connect", set_sqlite_pragma)
    DBSession = sqlalchemy.orm.sessionmaker(bind=engine)
    session = DBSession()
    Base.metadata.create_all(engine)
    return session


# enable foreign key constraints for sqlite backends
# http://docs.sqlalchemy.org/en/rel_0_9/dialects/sqlite.html#foreign-key-support
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
