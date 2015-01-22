import StringIO
import datetime
import json
import logging
import logging.config
import falcon.testing

import modelstatus.orm

config_file_contents = """
[wdb]
host=127.0.0.1

[formatters]
keys=default

[formatter_default]
class=logging.Formatter

[handlers]
keys=

[loggers]
keys=root

[logger_root]
handlers=
"""


base_api_url = '/modelstatus/v0'

class TestBase(falcon.testing.TestBase):
    def setup_database_fixture(self):
        for model in ['arome25', 'ecdet']:
            for i in xrange(1, 2):
                model_run = modelstatus.orm.ModelRun(
                    reference_time=datetime.datetime.utcfromtimestamp(0),
                    data_provider=model,
                    version=i
                )
                data = modelstatus.orm.Data(
                    model_run=model_run,
                    format='netcdf4',
                    href='/dev/null'
                )
                self.resource.orm.add(model_run)
                self.resource.orm.add(data)
        self.resource.orm.commit()

    def decode_body(self, body):
        return json.loads(body[0])




def get_test_logger():
    config_file = StringIO.StringIO(config_file_contents)
    logging.config.fileConfig(config_file, disable_existing_loggers=True)
    return logging.getLogger()

def get_api_base_url():
    return base_api_url
