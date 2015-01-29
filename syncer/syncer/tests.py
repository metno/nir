# coding: utf-8

import unittest
import ConfigParser
import StringIO
import datetime

import syncer
import syncer.wdb
import syncer.wdb2ts
import syncer.rest
import syncer.exceptions

config_file_contents = """
[wdb2ts]
base_url=http://localhost/metno-wdb2ts
services=proffecepsforecast,aromeecepsforecast

[wdb]
host=127.0.0.1
ssh_user=wdb

[model_foo]
data_provider=bar
data_uri_pattern=(foo|bar??)
load_program=netcdfLoad
load_config=/etc/netcdfload/arome.config

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


class WDB2TS(unittest.TestCase):
    VALID_STATUS_XML = """<status>
<updateid>aromeecepsforecast_1_9</updateid>
<defined_dataproviders>
<dataprovider>
<name>statkart.no</name>
</dataprovider>
<dataprovider>
<name>arome_metcoop_2500m</name>
</dataprovider>
<dataprovider>
<name>arome_metcoop_2500m_temperature_corrected</name>
</dataprovider>
<dataprovider>
<name>proff.default</name>
</dataprovider>
<dataprovider>
<name>met eceps small domain v.1.0</name>
</dataprovider>
<dataprovider>
<name>met eceps large domain v.1.0</name>
</dataprovider>
</defined_dataproviders>
</status>
"""

    def setUp(self):
        config_file = StringIO.StringIO(config_file_contents)
        config = syncer.Configuration()
        config.load(config_file)
        wdb2ts_services = [s.strip() for s in config.get('wdb2ts', 'services').split(',')]
        self.wdb2ts = syncer.wdb2ts.WDB2TS(config.get('wdb2ts', 'base_url'), wdb2ts_services)

    def test_request_status(self):

        with self.assertRaises(syncer.exceptions.WDB2TSRequestFailedException):
            self.wdb2ts.request_status('proffecepsforecast')

    def test_get_request(self):
        with self.assertRaises(syncer.exceptions.WDB2TSRequestFailedException):
            self.wdb2ts._get_request('http://test/testing')

    def test_data_providers_from_status_response(self):
        providers = syncer.wdb2ts.WDB2TS.data_providers_from_status_response(WDB2TS.VALID_STATUS_XML)

        self.assertIn('arome_metcoop_2500m', providers)

    def test_set_status_for_service(self):

        status = self.wdb2ts.set_status_for_service('aromeecepsforecast', WDB2TS.VALID_STATUS_XML)

        self.assertIn('arome_metcoop_2500m', status['data_providers'])


class WDBTest(unittest.TestCase):
    VALID_MODEL_FIXTURE = {
        'data_provider': 'arome_metcoop_2500m',
        'data_uri_pattern': '(foo|bar??)',
        'load_program': 'netcdfLoad',
        'load_config': '/etc/netcdfload/arome.config'
    }

    VALID_MODEL_RUN_FIXTURE = {
        'id': 1,
        'data_provider': 'arome_metcoop_2500m',
        'reference_time': '2015-01-19T16:04:40+0000',
        'created_date': '2015-01-19T16:04:40+0000',
        'version': 1337,
        'data': [
            {
                'model_run_id': 1,
                'id': '/modelstatus/v0/data1',
                'format': 'netcdf4',
                'href': 'opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc',
                'created_time': '2015-01-12T08:36:03Z'
            }
        ]
    }

    def setUp(self):
        self.wdb = syncer.wdb.WDB('localhost', 'test')
        self.model = syncer.Model(self.VALID_MODEL_FIXTURE)
        self.model_run = syncer.rest.ModelRun(self.VALID_MODEL_RUN_FIXTURE)

    def test_create_ssh_command(self):
        cmd = self.wdb.create_ssh_command(['ls'])

        self.assertEqual(" ".join(cmd), 'ssh test@localhost ls')

    def test_execute_command(self):
        results = syncer.wdb.WDB.execute_command('/bin/false')

        self.assertEqual(results[0], 1)

    def test_create_load_command(self):
        cmd = syncer.wdb.WDB.create_load_command(
            self.model,
            '/opdata/arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc'
        )
        self.assertEqual(" ".join(cmd), "netcdfLoad --dataprovider arome_metcoop_2500m -c /etc/netcdfload/arome.config --loadPlaceDefinition /opdata/arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc")

    def test_convert_opdata_uri_to_file(self):
        filepath = syncer.wdb.WDB.convert_opdata_uri_to_file('opdata:///nwparc/eps25/eps25_lqqt_probandltf_1_2015012600Z.nc')
        self.assertEqual(filepath, '/opdata/nwparc/eps25/eps25_lqqt_probandltf_1_2015012600Z.nc')

    def test_load_modelfile(self):
        with self.assertRaises(syncer.exceptions.WDBLoadFailed):
            self.wdb.load_modelfile(self.model, self.model_run)


class CollectionTest(unittest.TestCase):
    BASE_URL = 'http://localhost'

    def setUp(self):
        self.store = syncer.rest.ModelRunCollection(self.BASE_URL, True)

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

    def setUp(self):
        config_file = StringIO.StringIO(config_file_contents)
        self.config = syncer.Configuration()
        self.config.load(config_file)
        self.wdb = syncer.wdb.WDB(self.config.get('wdb', 'host'), self.config.get('wdb', 'ssh_user'))

        wdb2ts_services = [s.strip() for s in self.config.get('wdb2ts', 'services')]
        self.wdb2ts = syncer.wdb2ts.WDB2TS(self.config.get('wdb2ts', 'base_url'), wdb2ts_services)

    def test_instance(self):
        models = set()
        model_run_collection = syncer.rest.ModelRunCollection('http://localhost', True)
        data_collection = syncer.rest.DataCollection('http://localhost', True)
        zmq = syncer.zeromq.ZMQSubscriber('ipc://null')
        syncer.Daemon(self.config, models, zmq, self.wdb, self.wdb2ts, model_run_collection, data_collection)

    def test_instance_model_type_error(self):
        models = ['invalid type']
        with self.assertRaises(TypeError):
            syncer.Daemon(self.config, models)

    def test_instance_model_class_error(self):
        models = set([object()])
        with self.assertRaises(TypeError):
            syncer.Daemon(self.config, models)


class ModelTest(unittest.TestCase):
    VALID_FIXTURE = {
        'data_provider': 'bar',
        'data_uri_pattern': '(foo|bar??)',
        'load_program': 'netcdfLoad',
        'load_config': '/etc/netcdfload/arome.config'
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


class ModelRunTest(unittest.TestCase):
    """Tests the syncer.rest.ModelRun resource"""

    VALID_FIXTURE = {
        'id': 1,
        'data_provider': 'arome_metcoop_2500m',
        'reference_time': '2015-01-19T16:04:40+0000',
        'created_date': '2015-01-19T16:04:40+0000',
        'data': [],
        'version': 1337,
    }

    def test_initialize_with_invalid_reference_time(self):
        invalid_fixture = self.VALID_FIXTURE
        invalid_fixture['reference_time'] = 'in a galaxy far, far away'
        with self.assertRaises(ValueError):
            syncer.rest.ModelRun(invalid_fixture)

    def test_initialize_with_correct_data(self):
        model_run = syncer.rest.ModelRun(self.VALID_FIXTURE)
        self.assertIsInstance(model_run.id, int)
        self.assertIsInstance(model_run.data_provider, str)
        self.assertIsInstance(model_run.reference_time, datetime.datetime)
        self.assertIsInstance(model_run.version, int)


class ZeroMQTest(unittest.TestCase):
    """
    Tests the ZeroMQ classes
    """
    def setUp(self):
        self.zmq = syncer.zeromq.ZMQSubscriber('ipc://null')

    def test_decode_event_ok(self):
        string = 'foo 123'
        event = self.zmq.decode_event(string)
        self.assertEqual(event.resource, 'foo')
        self.assertEqual(event.id, 123)

    def test_decode_event_invalid_id(self):
        string = 'foo bar'
        event = self.zmq.decode_event(string)
        self.assertIsNone(event)

    def test_decode_event_invalid_format(self):
        string = 'foobar'
        event = self.zmq.decode_event(string)
        self.assertIsNone(event)

    def test_zmqevent_class(self):
        event = syncer.zeromq.ZMQEvent(id=123, resource='foo', bar='baz')
        self.assertEqual(event.bar, 'baz')
        self.assertEqual(event.resource, 'foo')
        self.assertEqual(event.id, 123)

if __name__ == '__main__':
    unittest.main()
