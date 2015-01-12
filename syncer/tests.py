#!/usr/bin/env python2.7

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
        parser = ConfigParser.SafeConfigParser()
        syncer.read_config_file(parser, config_file)
        self.assertEqual(parser.get('wdb', 'host'), '127.0.0.1')

    def test_argparse_config(self):
        parser = syncer.create_argument_parser()
        syncer.setup_argument_parser(parser)
        args = syncer.parse_args(parser, ['--config', '/dev/null'])
        self.assertEqual(args.config, '/dev/null')

if __name__ == '__main__':
    unittest.main()
