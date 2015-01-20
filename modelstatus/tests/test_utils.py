import StringIO
import logging
import logging.config

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

def get_test_logger():

    config_file = StringIO.StringIO(config_file_contents)
    logging.config.fileConfig(config_file, disable_existing_loggers=True)
    return logging.getLogger()

def get_api_base_url():

    return base_api_url
