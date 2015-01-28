import sqlalchemy
import modelstatus.orm
import os


engine = sqlalchemy.create_engine(os.environ['CONNECTION_STRING'])

# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
modelstatus.orm.Base.metadata.create_all(engine)
