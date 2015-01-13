#!/usr/bin/env python2.7

import logging
import argparse
import unittest
import ConfigParser
import StringIO
import syncer

config_file_contents = """
[wdb]
host=127.0.0.1
"""

class SyncerTest(unittest.TestCase):
    def test_read_config_file(self):
        config_file = StringIO.StringIO(config_file_contents)
        parser = syncer.create_config_parser()
        syncer.read_config_file(parser, config_file)
        self.assertEqual(parser.get('wdb', 'host'), '127.0.0.1')

    def test_argparse_config(self):
        parser = syncer.create_argument_parser()
        syncer.setup_argument_parser(parser)
        args = syncer.parse_args(parser, ['--config', '/dev/null'])
        self.assertEqual(args.config, '/dev/null')

    def test_logging(self):
        """The test is that no exception is thrown."""
        syncer.setup_logging('/dev/null', 'ERROR')
        logging.error('test')

if __name__ == '__main__':
    unittest.main()
