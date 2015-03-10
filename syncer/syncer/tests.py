# coding: utf-8

import copy
import unittest
import ConfigParser
import StringIO
import datetime
import dateutil

import syncer
import syncer.wdb
import syncer.wdb2ts
import syncer.rest
import syncer.utils
import syncer.exceptions

config_file_contents = """
[wdb2ts]
base_url=http://localhost/metno-wdb2ts
services=proffecepsforecast,aromeecepsforecast

[wdb]
host=127.0.0.1
ssh_user=wdb

[model_foo]
data_provider=arome_metcoop_2500m
data_provider_group=arome
data_file_count=1
data_uri_pattern=(arome_metcoop|bar??)
model_run_age_warning=30
model_run_age_critical=60
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


VALID_MODEL_RUN_FIXTURE = {
    'id': 1,
    'data_provider': 'arome_metcoop_2500m',
    'reference_time': '2015-01-19T16:04:40Z',
    'created_date': '2015-01-19T16:04:40Z',
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


VALID_MODEL_FIXTURE = {
    'data_provider': 'arome_metcoop_2500m',
    'data_provider_group': 'arome',
    'data_file_count': 1,
    'data_uri_pattern': '(arome_metcoop|bar??)',
    'model_run_age_warning': 30,
    'model_run_age_critical': 60,
    'load_program': 'netcdfLoad',
    'load_config': '/etc/netcdfload/arome.config'
}


VALID_STATE_HASH = {
    "models": [
        {
            "_available_model_run_initialized": True,
            "available_model_run": {
                "created_date": "2015-03-09T14:29:03.689297Z",
                "data": [
                    {
                        "format": "netcdf",
                        "href": "opdata:///arome2_5/AROME_MetCoOp_12_fp.nc",
                        "id": 4807,
                        "model_run_id": 801
                    }
                ],
                "data_provider": "arome_metcoop_2500m",
                "id": 801,
                "reference_time": "2015-03-09T12:00:00Z",
                "version": 1
            },
            "available_updated": "2015-03-09T15:22:39.265798Z",
            "data_provider": "arome_metcoop_2500m",
            "model_run_age_critical": 60,
            "model_run_age_warning": 30,
            "model_run_version": {
                "2015-03-09T12:00:00Z": 8
            },
            "wdb2ts_model_run": {
                "created_date": "2015-03-09T14:29:03.689297Z",
                "data": [
                    {
                        "format": "netcdf",
                        "href": "opdata:///arome2_5/AROME_MetCoOp_12_fp.nc",
                        "id": 4807,
                        "model_run_id": 801
                    }
                ],
                "data_provider": "arome_metcoop_2500m",
                "id": 801,
                "reference_time": "2015-03-09T12:00:00Z",
                "version": 1
            },
            "wdb2ts_updated": "2015-03-09T15:29:39.265798Z",
            "wdb_model_run": {
                "created_date": "2015-03-09T14:29:03.689297Z",
                "data": [
                    {
                        "format": "netcdf",
                        "href": "opdata:///arome2_5/AROME_MetCoOp_12_fp.nc",
                        "id": 4807,
                        "model_run_id": 801
                    }
                ],
                "data_provider": "arome_metcoop_2500m",
                "id": 801,
                "reference_time": "2015-03-09T12:00:00Z",
                "version": 1
            },
            "wdb_updated": "2015-03-09T15:28:39.265798Z",
        }
    ]
}


ZEROMQ_PROTOCOL_VERSION = [1, 1, 0]


class SerializeBaseTest(unittest.TestCase):
    def setUp(self):
        self.class_ = syncer.utils.SerializeBase()

    def test_serialize_datetime_utc(self):
        dt = datetime.datetime.utcfromtimestamp(3661).replace(tzinfo=dateutil.tz.tzutc())
        dt_string = self.class_._serialize_datetime(dt)
        self.assertEqual(dt_string, '1970-01-01T01:01:01Z')

    def test_serialize_datetime_cet(self):
        dt = datetime.datetime.utcfromtimestamp(3661).replace(tzinfo=dateutil.tz.gettz('Europe/Oslo'))
        dt_string = self.class_._serialize_datetime(dt)
        self.assertEqual(dt_string, '1970-01-01T00:01:01Z')

    def test_serialize_datetime_reject_naive(self):
        dt = datetime.datetime.utcfromtimestamp(3661)
        with self.assertRaises(ValueError):
            self.class_._serialize_datetime(dt)


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
        base_url = config.get('wdb2ts', 'base_url')
        wdb2ts_services = [s.strip() for s in config.get('wdb2ts', 'services').split(',')]
        self.wdb2ts = syncer.wdb2ts.WDB2TS(base_url, wdb2ts_services)

    def test_data_providers_from_status_response(self):
        providers = syncer.wdb2ts.WDB2TS.data_providers_from_status_response(self.VALID_STATUS_XML)
        self.assertIn('arome_metcoop_2500m', providers)

    def test_set_status_for_service(self):
        status = self.wdb2ts.set_status_for_service('aromeecepsforecast', self.VALID_STATUS_XML)
        self.assertIn('arome_metcoop_2500m', status['data_providers'])

    def test_get_update_url(self):
        url = self.wdb2ts.get_update_url('aromeecepsforecast', 'arome_metcoop_2500m', '2015-01-29T00:00:00Z', 1)
        self.assertEqual('http://localhost/metno-wdb2ts/aromeecepsforecastupdate?arome_metcoop_2500m=2015-01-29T00:00:00Z,1', url)


class WDBTest(unittest.TestCase):

    def setUp(self):
        self.wdb = syncer.wdb.WDB('localhost', 'test')
        self.model = syncer.Model(VALID_MODEL_FIXTURE)
        self.model_run = syncer.rest.ModelRun(VALID_MODEL_RUN_FIXTURE)

    def test_create_ssh_command(self):
        cmd = self.wdb.create_ssh_command(['ls'])

        self.assertEqual(" ".join(cmd), 'ssh test@localhost ls')

    def test_execute_command(self):
        results = syncer.wdb.WDB.execute_command('/bin/false')

        self.assertEqual(results[0], 1)

    def test_create_load_command(self):
        cmd = syncer.wdb.WDB.create_load_command(
            self.model,
            self.model_run,
            '/opdata/arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc'
        )
        self.assertEqual(" ".join(cmd), "netcdfLoad --dataprovider 'arome_metcoop_2500m' -c /etc/netcdfload/arome.config --loadPlaceDefinition --dataversion 1337 /opdata/arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc")

    def test_convert_opdata_uri_to_file(self):
        filepath = syncer.wdb.WDB.convert_opdata_uri_to_file('opdata:///nwparc/eps25/eps25_lqqt_probandltf_1_2015012600Z.nc')
        self.assertEqual(filepath, '/opdata/nwparc/eps25/eps25_lqqt_probandltf_1_2015012600Z.nc')

    def test_create_cache_query(self):
        query = syncer.wdb.WDB.create_cache_query(self.model_run)
        self.assertEqual(query, "SELECT wci.begin('wdb'); SELECT wci.cacheQuery(array['arome_metcoop_2500m'], NULL, 'exact 2015-01-19T16:04:40Z', NULL, NULL, NULL, array[-1])")

    def test_create_analyze_query(self):
        query = syncer.wdb.WDB.create_analyze_query()
        self.assertEqual(query, "ANALYZE")

    def test_create_cache_model_run_command(self):
        cmd_list = self.wdb.create_cache_model_run_command(self.model_run)
        cmd = ' '.join(cmd_list)
        self.assertEqual(cmd, "ssh test@localhost psql -c \"SELECT wci.begin('wdb'); SELECT wci.cacheQuery(array['arome_metcoop_2500m'], NULL, 'exact 2015-01-19T16:04:40Z', NULL, NULL, NULL, array[-1]); ANALYZE;\"")


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

    def make_daemon(self):
        models = set([syncer.Model(VALID_MODEL_FIXTURE)])
        model_run_collection = syncer.rest.ModelRunCollection('http://localhost', True)
        data_collection = syncer.rest.DataCollection('http://localhost', True)
        zmq_subscriber = syncer.zeromq.ZMQSubscriber('ipc://null', 30, 30)
        zmq_agent = syncer.zeromq.ZMQAgent()
        tick = 300
        state_file = '/dev/null'
        return syncer.Daemon(self.config, models, zmq_subscriber, zmq_agent, self.wdb, self.wdb2ts, model_run_collection, data_collection, tick, state_file)

    def test_instance(self):
        self.make_daemon()

    def test_read_state_file(self):
        daemon = self.make_daemon()
        state = daemon.read_state_file()
        self.assertEqual(state, {})

    def test_write_state_file(self):
        daemon = self.make_daemon()
        daemon.write_state_file({})

    def test_load_state(self):
        daemon = self.make_daemon()
        daemon.load_state(VALID_STATE_HASH)
        self.assertEqual(len(daemon.models), 1)
        for model in daemon.models:
            self.assertEqual(model.available_model_run.version, 1)
            self.assertEqual(model.available_model_run.id, 801)
            self.assertEqual(model.available_model_run.data[0].id, 4807)
            self.assertFalse(model.has_pending_wdb_load())
            self.assertFalse(model.has_pending_wdb2ts_update())
            model.set_wdb2ts_model_run(None)
            self.assertTrue(model.has_pending_wdb2ts_update())
            model.set_wdb_model_run(None)
            self.assertTrue(model.has_pending_wdb_load())

    def test_make_state(self):
        pass

    def test_instance_model_type_error(self):
        models = ['invalid type']
        with self.assertRaises(TypeError):
            syncer.Daemon(self.config, models)

    def test_instance_model_class_error(self):
        models = set([object()])
        with self.assertRaises(TypeError):
            syncer.Daemon(self.config, models)


class ModelTest(unittest.TestCase):
    def setUp(self):
        self.config_file = StringIO.StringIO(config_file_contents)
        self.config = syncer.Configuration()
        self.config.load(self.config_file)

    def get_model(self):
        return syncer.Model(VALID_MODEL_FIXTURE)

    def get_model_run(self):
        return syncer.rest.ModelRun(VALID_MODEL_RUN_FIXTURE)

    def test_data_from_config_section(self):
        data = syncer.Model.data_from_config_section(self.config, 'model_foo')
        self.assertEqual(data, VALID_MODEL_FIXTURE)

    def test_instantiate(self):
        model = self.get_model()
        for key, value in VALID_MODEL_FIXTURE.iteritems():
            self.assertEqual(getattr(model, key), value)

    def test_instantiate_no_model_run_age_threshold(self):
        fixture = copy.deepcopy(VALID_MODEL_FIXTURE)
        del fixture['model_run_age_warning']
        del fixture['model_run_age_critical']
        syncer.Model(fixture)

    def test_serialize(self):
        model = self.get_model()
        serialized = model.serialize()
        for key in [
                'available_model_run', 'wdb_model_run', 'wdb2ts_model_run',
                'available_updated', 'wdb_updated', 'wdb2ts_updated']:
            self.assertEqual(serialized[key], None)
            del serialized[key]

        self.assertEqual(serialized['model_run_version'], {})
        del serialized['model_run_version']
        del serialized['_available_model_run_initialized']

        for key, value in serialized.iteritems():
            self.assertEqual(serialized[key], VALID_MODEL_FIXTURE[key])

    def test_serialize_missing_values(self):
        fixture = copy.deepcopy(VALID_MODEL_FIXTURE)
        params = ['model_run_age_warning', 'model_run_age_critical']
        for param in params:
            del fixture[param]
        model = syncer.Model(fixture)
        serialized = model.serialize()
        for param in params:
            self.assertEqual(serialized[param], None)

    def test_data_from_config_section_missing_key(self):
        with self.assertRaises(ConfigParser.NoOptionError):
            syncer.Model.data_from_config_section(self.config, 'model_bar')

    def test_set_available_model_run(self):
        model = self.get_model()
        model_run = self.get_model_run()
        model.set_available_model_run(model_run)
        self.assertEqual(model_run, model.available_model_run)
        self.assertTrue(model.model_run_initialized())

    def test_set_wdb_model_run(self):
        model = self.get_model()
        model_run = self.get_model_run()
        model.set_wdb_model_run(model_run)
        self.assertEqual(model_run, model.wdb_model_run)

    def test_set_wdb2ts_model_run(self):
        model = self.get_model()
        model_run = self.get_model_run()
        model.set_wdb2ts_model_run(model_run)
        self.assertEqual(model_run, model.wdb2ts_model_run)

    def test_set_must_update_wdb(self):
        """
        Test that forcing WDB loading works and that the option is reset when it is done.
        """
        model = self.get_model()
        model_run = self.get_model_run()
        model.set_available_model_run(model_run)
        model.set_wdb_model_run(model_run)
        self.assertEqual(model.has_pending_wdb_load(), False)
        model.set_must_update_wdb(True)
        self.assertEqual(model.has_pending_wdb_load(), True)
        model.set_wdb_model_run(model_run)
        self.assertEqual(model.has_pending_wdb_load(), False)

    def test_set_must_update_wdb2ts(self):
        """
        Test that forcing WDB updates works and that the option is reset when it is done.
        """
        model = self.get_model()
        model_run = self.get_model_run()
        model.set_available_model_run(model_run)
        model.set_wdb_model_run(model_run)
        model.set_wdb2ts_model_run(model_run)
        self.assertEqual(model.has_pending_wdb2ts_update(), False)
        model.set_must_update_wdb2ts(True)
        self.assertEqual(model.has_pending_wdb2ts_update(), True)
        model.set_wdb2ts_model_run(model_run)
        self.assertEqual(model.has_pending_wdb2ts_update(), False)

    def test_set_invalid_data(self):
        """
        Test that the various set_ functions only accepts a ModelRun object.
        """
        model = self.get_model()
        model_run = object()
        with self.assertRaises(TypeError):
            model.set_available_model_run(model_run)
        with self.assertRaises(TypeError):
            model.set_wdb_model_run(model_run)
        with self.assertRaises(TypeError):
            model.set_wdb2ts_model_run(model_run)

    def test_get_model_run_key(self):
        model = self.get_model()
        model_run = self.get_model_run()
        key = model.get_model_run_key(model_run)
        self.assertEqual(key, VALID_MODEL_RUN_FIXTURE['reference_time'])

    def test_get_model_run_version(self):
        model = self.get_model()
        model_run = self.get_model_run()
        version = model.get_model_run_version(model_run)
        self.assertEqual(version, 1337)

    def test_get_internal_model_run_version(self):
        model = self.get_model()
        model_run = self.get_model_run()
        version = model.get_internal_model_run_version(model_run)
        self.assertEqual(version, 0)

    def test_set_model_run_version(self):
        model = self.get_model()
        model_run = self.get_model_run()
        ref_version = 2674
        model.set_model_run_version(model_run, 1337)
        version = model.get_model_run_version(model_run)
        self.assertEqual(version, ref_version)

    def test_increment_model_run_version(self):
        model = self.get_model()
        model_run = self.get_model_run()
        model.increment_model_run_version(model_run)
        version = model.get_model_run_version(model_run)
        self.assertEqual(version, 1338)


class ModelRunTest(unittest.TestCase):
    """Tests the syncer.rest.ModelRun resource"""

    def test_initialize_with_invalid_reference_time(self):
        invalid_fixture = copy.deepcopy(VALID_MODEL_RUN_FIXTURE)
        invalid_fixture['reference_time'] = 'in a galaxy far, far away'
        with self.assertRaises(ValueError):
            syncer.rest.ModelRun(invalid_fixture)

    def test_initialize_with_correct_data(self):
        model_run = syncer.rest.ModelRun(VALID_MODEL_RUN_FIXTURE)
        self.assertIsInstance(model_run.id, int)
        self.assertIsInstance(model_run.data_provider, str)
        self.assertIsInstance(model_run.reference_time, datetime.datetime)
        self.assertIsInstance(model_run.version, int)


class ZeroMQTest(unittest.TestCase):
    """
    Tests the ZeroMQ classes
    """
    def setUp(self):
        self.zmq = syncer.zeromq.ZMQSubscriber('ipc://test_zmq', 30, 30)
        self.controller = syncer.zeromq.ZMQController('ipc://test_ctl')

    def test_zmqevent_ok(self):
        data = {
            'version': ZEROMQ_PROTOCOL_VERSION,
            'type': 'resource',
            'resource': 'foo',
            'id': 123,
        }
        event = syncer.zeromq.ZMQEvent.factory(**data)
        self.assertIsInstance(event, syncer.zeromq.ZMQResourceEvent)
        self.assertEqual(event.resource, 'foo')
        self.assertEqual(event.type, 'resource')
        self.assertEqual(event.id, 123)
        self.assertEqual(event.version, ZEROMQ_PROTOCOL_VERSION)

    def test_zmqevent_invalid_id(self):
        data = {
            'version': ZEROMQ_PROTOCOL_VERSION,
            'type': 'resource',
            'resource': 'foo',
            'id': 'bar',
        }
        with self.assertRaises(syncer.exceptions.ZMQEventBadId):
            syncer.zeromq.ZMQEvent.factory(**data)

    def test_zmqevent_invalid_resource(self):
        data = {
            'version': ZEROMQ_PROTOCOL_VERSION,
            'type': 'resource',
            'resource': '',
            'id': 9,
        }
        with self.assertRaises(syncer.exceptions.ZMQEventBadResource):
            syncer.zeromq.ZMQEvent.factory(**data)

    def test_zmqevent_missing_type(self):
        data = {
            'version': ZEROMQ_PROTOCOL_VERSION,
            'resource': 'foo',
            'id': 23,
        }
        with self.assertRaises(syncer.exceptions.ZMQEventIncomplete):
            syncer.zeromq.ZMQEvent.factory(**data)

    def test_zmqevent_missing_id(self):
        data = {
            'version': ZEROMQ_PROTOCOL_VERSION,
            'type': 'resource',
            'resource': 'foo',
        }
        with self.assertRaises(syncer.exceptions.ZMQEventIncomplete):
            syncer.zeromq.ZMQEvent.factory(**data)

    def test_zmqevent_missing_resource(self):
        data = {
            'version': ZEROMQ_PROTOCOL_VERSION,
            'type': 'resource',
            'id': 456,
        }
        with self.assertRaises(syncer.exceptions.ZMQEventIncomplete):
            syncer.zeromq.ZMQEvent.factory(**data)

    def test_zmqevent_unsupported_version(self):
        data = {
            'version': [2, 1, 3],
            'type': 'resource',
            'resource': 'foo',
            'id': 456,
        }
        with self.assertRaises(syncer.exceptions.ZMQEventUnsupportedVersion):
            syncer.zeromq.ZMQEvent.factory(**data)

    def test_zmqevent_bad_version(self):
        data = {
            'version': 'bleeding edge',
            'type': 'resource',
            'resource': 'foo',
            'id': 456,
        }
        with self.assertRaises(syncer.exceptions.ZMQEventUnsupportedVersion):
            syncer.zeromq.ZMQEvent.factory(**data)

    def test_command_non_existing(self):
        data = self.controller.exec_command({'command': 'foobarbaz'})
        self.assertEqual(data['status'], 2)
        self.assertEqual(data['data'], [u"Invalid command 'foobarbaz'"])

    def test_command_hello(self):
        data = self.controller.exec_command({'command': 'hello'})
        self.assertEqual(data['status'], 0)
        self.assertEqual(data['data'], ['Hello, world!'])


if __name__ == '__main__':
    unittest.main()
