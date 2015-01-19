import unittest
import StringIO
import modelstatus.app


config_file_contents = """

[loggers]
keys=root

[handlers]
keys=stdout,syslog

[formatters]
keys=default

[formatter_default]
format=%(asctime)s (%(levelname)s) %(message)s
datefmt=
class=logging.Formatter

[handler_stdout]
class=logging.StreamHandler
formatter=default
args=()

[handler_syslog]
class=logging.handlers.SysLogHandler
formatter=default
args=(('localhost', handlers.SYSLOG_UDP_PORT), handlers.SysLogHandler.LOG_USER,)

[logger_root]
level=DEBUG
handlers=stdout,syslog
qualname=syncer
"""

class TestApp(unittest.TestCase):

    def setUp(self):
        self.config_file = StringIO.StringIO(config_file_contents)

    def test_setup_logger(self):
        logger = modelstatus.app.setup_logger(self.config_file)
        
        self.assertEqual(logger.__class__.__name__, 'RootLogger')


    def test_parse_arguments(self):

        args = modelstatus.app.parse_arguments(['--config', '/dev/null'])
        self.assertEqual(args.config, '/dev/null')



if __name__ == '__main__':
    unittest.main()
