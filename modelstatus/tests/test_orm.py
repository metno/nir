import sqlalchemy.engine
import sqlalchemy.orm.session
import sqlalchemy.orm 
import sqlalchemy.exc
import datetime
import unittest
import modelstatus.orm

class TestOrm(unittest.TestCase):
    def setUp(self):
        self.engine = sqlalchemy.engine.create_engine('sqlite://') #In memory db
        # Bind the engine to the metadata of the Base class so that the
        # declaratives can be accessed through a DBSession instance
        modelstatus.orm.Base.metadata.bind = self.engine 
        DBSession = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session = DBSession()
        modelstatus.orm.Base.metadata.create_all(self.engine)

    def tearDown(self):
        pass
        
    def test_db_lookup(self):
        date_str = '2015-01-13 12:00:00Z'
        reference_time = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%SZ")
        model_run = modelstatus.orm.ModelRun(
            reference_time = reference_time, 
            data_provider = "arome25");
        self.session.add(model_run)
        self.session.commit()

        self.assertEqual(1, self.session.query(modelstatus.orm.ModelRun).count())

        mr = self.session.query(modelstatus.orm.ModelRun).first()
        self.assertEqual(mr.reference_time.strftime("%Y-%m-%d %H:%M:%SZ"), date_str)

    def test_join(self):
        date_str = '2015-01-13 12:00:00Z'
        reference_time = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%SZ")
        model_run = modelstatus.orm.ModelRun(
            reference_time=reference_time, 
            data_provider="arome25"
            )

        model_run2 = modelstatus.orm.ModelRun(
            reference_time = reference_time, 
            data_provider="arome25"
            )
        
        self.session.add(model_run)
        self.session.commit()
        self.session.add(model_run2)
        self.session.commit()
        data1 = modelstatus.orm.Data(
            uri='opdata:///any/ec1.nc', 
            format='netcdf4',
            model_run=model_run
            )

        self.session.add(data1)

        data2 = modelstatus.orm.Data(
            uri='opdata:///any/ec2.nc', 
            format='netcdf4',
            model_run=model_run
            )
        self.session.add(data2)

        self.session.commit()
        
        response_count = (self.session.query(modelstatus.orm.Data)
                     .filter(modelstatus.orm.Data.model_run == model_run).count())
        self.assertEqual(2,response_count)
        
    def test_null(self):
        model_run = modelstatus.orm.ModelRun();
        self.session.add(model_run)
        self.assertRaises(sqlalchemy.exc.IntegrityError, self.session.commit)
        

if __name__ == '__main__':
    unittest.main()

