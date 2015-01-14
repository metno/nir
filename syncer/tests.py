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

class SyncerTest(unittest.TestCase):
    def setUp(self):
        self.config_file = StringIO.StringIO(config_file_contents)

    def test_read_config_file(self):
        parser = syncer.create_config_parser()
        syncer.read_config_file(parser, self.config_file)
        self.assertEqual(parser.get('wdb', 'host'), '127.0.0.1')

    def test_argparse_config(self):
        parser = syncer.create_argument_parser()
        syncer.setup_argument_parser(parser)
        args = syncer.parse_args(parser, ['--config', '/dev/null'])
        self.assertEqual(args.config, '/dev/null')

    def test_setup_logging(self):
        syncer.setup_logging(self.config_file)

class CollectionTest(unittest.TestCase):
    BASE_URL = 'http://localhost'

    def setUp(self):
        self.store = syncer.ModelRunCollection(self.BASE_URL)

    def test_get_collection_url(self):
        self.assertEqual(self.store.get_collection_url(), "%s/model_run" % self.BASE_URL)

    def test_get_resource_url(self):
        id = 48949832
        self.assertEqual(self.store.get_resource_url(id), "%s/model_run/%d" % (self.BASE_URL, id))

    def test_unserialize(self):
        json_string = '{"foo":"bar"}'
        json_data = {"foo": "bar"}
        self.assertEqual(self.store.unserialize(json_string), json_data)


if __name__ == '__main__':
    unittest.main()
