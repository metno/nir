# coding: utf-8

import logging
import argparse
import unittest
import ConfigParser
import StringIO

import syncer
import syncer.rest

config_file_contents = """
[wdb]
host=127.0.0.1

[model_foo]
data_provider=bar

[model_bar]
shizzle=foo

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

    def test_setup_logging(self):
        syncer.setup_logging(self.config_file)


class ConfigurationTest(unittest.TestCase):
    def setUp(self):
        self.config_file = StringIO.StringIO(config_file_contents)
        self.config = syncer.Configuration()

    def test_read_config_file(self):
        self.config.load(self.config_file)
        self.assertEqual(self.config.config_parser.get('wdb', 'host'), '127.0.0.1')

    def test_argparse_config(self):
        args = self.config.argument_parser.parse_args(['--config', '/dev/null'])
        self.assertEqual(args.config, '/dev/null')


class CollectionTest(unittest.TestCase):
    BASE_URL = 'http://localhost'

    def setUp(self):
        self.store = syncer.rest.ModelRunCollection(self.BASE_URL)

    def test_get_collection_url(self):
        self.assertEqual(self.store.get_collection_url(), "%s/model_run" % self.BASE_URL)

    def test_get_resource_url(self):
        id = 48949832
        self.assertEqual(self.store.get_resource_url(id), "%s/model_run/%d" % (self.BASE_URL, id))

    def test_unserialize(self):
        json_string = '{"foo":"bar"}'
        json_data = {"foo": "bar"}
        self.assertEqual(self.store.unserialize(json_string), json_data)


class DaemonTest(unittest.TestCase):
    def test_instance(self):
        config = syncer.Configuration()
        models = set()
        model_run_collection = syncer.rest.ModelRunCollection('http://localhost')
        daemon = syncer.Daemon(config, models, model_run_collection)

    def test_instance_model_type_error(self):
        config = syncer.Configuration()
        models = ['invalid type']
        with self.assertRaises(TypeError):
            daemon = syncer.Daemon(config, models)

    def test_instance_model_class_error(self):
        config = syncer.Configuration()
        models = set([object()])
        with self.assertRaises(TypeError):
            daemon = syncer.Daemon(config, models)


class ModelTest(unittest.TestCase):
    VALID_FIXTURE = {
            'data_provider': 'bar'
            }

    def setUp(self):
        self.config_file = StringIO.StringIO(config_file_contents)
        self.config = syncer.Configuration()
        self.config.load(self.config_file)

    def test_data_from_config_section(self):
        data = syncer.Model.data_from_config_section(self.config, 'model_foo')
        self.assertEqual(data, self.VALID_FIXTURE)

    def test_instantiate(self):
        model = syncer.Model(self.VALID_FIXTURE)
        for key, value in self.VALID_FIXTURE.iteritems():
            self.assertEqual(getattr(model, key), value)

    def test_data_from_config_section_missing_key(self):
        with self.assertRaises(ConfigParser.NoOptionError):
            syncer.Model.data_from_config_section(self.config, 'model_bar')



if __name__ == '__main__':
    unittest.main()
