import os
import sys
import sqlalchemy.inspection
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy
import psycopg2
import datetime

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


class ModelRun(Base, SerializeBase):
    __tablename__ = 'model_run'
    __serializable__ = ['id', 'reference_time', 'data_provider', 'created_date', 'version', 'data']

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    reference_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True),
                                       nullable=False)
    data_provider = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    version = sqlalchemy.Column(sqlalchemy.Integer)
    created_date = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True),
                                     default=datetime.datetime.utcnow)
    __table_args__ = (sqlalchemy.UniqueConstraint('data_provider', 'reference_time', 'version'),)

    def _serialize_datetime(self, value):
        return value.isoformat()

    def serialize_reference_time(self, value):
        return self._serialize_datetime(value)

    def serialize_created_date(self, value):
        return self._serialize_datetime(value)

    def serialize_data(self, value):
        return [x.serialize() for x in value]


class Data(Base, SerializeBase):
    __tablename__ = 'data'
    __serializable__ = ['id', 'href', 'format', 'model_run_id']

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    href = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    format = sqlalchemy.Column(sqlalchemy.String(64), nullable=False)
    model_run_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('model_run.id'), nullable=False)
    model_run = sqlalchemy.orm.relationship(ModelRun, backref='data')


def get_sqlite_memory_session():
    engine = sqlalchemy.engine.create_engine('sqlite://')
    DBSession = sqlalchemy.orm.sessionmaker(bind=engine)
    session = DBSession()
    Base.metadata.create_all(engine)
    return session
