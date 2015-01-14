import os
import sys
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy
import psycopg2
import os

Base = sqlalchemy.ext.declarative.declarative_base() 
class ModelRun(Base):
    __tablename__ = 'model_run'
    # Here we define columns for the table model_run
    # Notice that each column is also a normal Python instance attribute.
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    reference_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=False), nullable=False)
    data_provider = sqlalchemy.Column(sqlalchemy.String, nullable=False)

class Data(Base):
    __tablename__ = 'data'
    # Here we define columns for the table data.
    # Notice that each column is also a normal Python instance attribute.
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    uri = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    format = sqlalchemy.Column(sqlalchemy.String(64), nullable=False)
    model_run_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('model_run.id'))
    model_run = sqlalchemy.orm.relationship(ModelRun)

