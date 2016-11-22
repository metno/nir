import unittest
from unittest.mock import Mock
import syncer.loading
from syncer.tests import fake_productstatus


class DefaultConfig(object):
    def __init__(self):
        self.data = {'syncer': {'models': 'whatever',
                                'state_database_file': ':memory:'},
                     'productstatus': {'url': '',
                                       'verify_ssl': '0',
                                       'client_id': '',
                                       'group_id': '',
                                       'max_heartbeat_delay': '0'},
                     'wdb': {'host': '',
                             'user': ''},
                     'wdb2ts': {'base_url': '',
                                'services': ''},
                     'model_whatever': {'product': 'product1',
                                        'servicebackend': 'servicebackend1',
                                        'load_program': '/usr/lib/wdb/netcdfLoad',
                                        'data_provider': 'whatever',
                                        'model_run_age_warning': '30',
                                        'model_run_age_critical': '45'}}

    def get(self, section, key, default=None):
        try:
            return self.data[section][key]
        except KeyError:
            return default

    def section_keys(self, section_name):
        return [x[0] for x in self.data[section_name].items()]

    def section_options(self, section_name):
        return self.data[section_name]


class TestingDataLoader(syncer.loading.DataLoader):
    def __init__(self, config=None):
        syncer.loading.DataLoader.__init__(self, config or DefaultConfig(), sleep_time_on_error=0)
        self.config = config
        self.wdb = Mock()
        self.wdb2ts = Mock()
        self.api = fake_productstatus.ProductStatus(open('productstatus.yml'))

    def _get_datainstances_belonging_to(self, productinstance):
        ret = []
        for d in productinstance.data:
            for di in d.datainstance:
                ret.append(di)
        return ret


def Any(cls):
    class Any(cls):
        def __eq__(self, other):
            return True
    return Any()


class StateDatabaseTest(unittest.TestCase):
    def test_create(self):
        loader = TestingDataLoader()
        loader.process()
        loader.wdb.load_model_file.assert_not_called()
        loader.wdb.cache_model_run.assert_not_called()
        loader.wdb2ts.update.assert_not_called()

    def test_load(self):
        loader = TestingDataLoader()
        loader._state_database.add_productinstance_to_be_processed(loader.api.productinstance['productinstance1'])
        loader.process()
        model = Any(set)
        loader.wdb.load_model_file.assert_called_once_with(loader.api.datainstance['datainstance1'], model)
        loader.wdb.cache_model_run.assert_called_once_with(loader.api.productinstance['productinstance1'], model)
        loader.wdb2ts.update.assert_called_once_with(loader.api.productinstance['productinstance1'], model)

        loader.wdb.reset_mock()
        loader.wdb2ts.reset_mock()
        loader.wdb.load_model_file.assert_not_called()
        loader.wdb.cache_model_run.assert_not_called()
        loader.wdb2ts.update.assert_not_called()

    def test_load_failed(self):
        loader = TestingDataLoader()
        loader._state_database.add_productinstance_to_be_processed(loader.api.productinstance['productinstance1'])
        loader.wdb.load_model_file.side_effect = syncer.exceptions.WDBLoadFailed('loading failed')
        model = Any(set)
        for i in range(3):
            loader.process()
            loader.wdb.load_model_file.assert_called_once_with(loader.api.datainstance['datainstance1'], model)
            loader.wdb.cache_model_run.assert_not_called()
            loader.wdb2ts.update.assert_not_called()
            loader.wdb.reset_mock()

    def test_load_failed_recovery(self):
        loader = TestingDataLoader()
        loader._state_database.add_productinstance_to_be_processed(loader.api.productinstance['productinstance1'])
        loader.wdb.load_model_file.side_effect = syncer.exceptions.WDBLoadFailed('loading failed')
        loader.process()
        model = Any(set)
        loader.wdb.load_model_file.assert_called_once_with(loader.api.datainstance['datainstance1'], model)
        loader.wdb.cache_model_run.assert_not_called()
        loader.wdb2ts.update.assert_not_called()

        loader.wdb.reset_mock()
        loader.wdb2ts.reset_mock()
        loader.wdb.load_model_file.side_effect = None
        loader.process()
        loader.wdb.load_model_file.assert_called_once_with(loader.api.datainstance['datainstance1'], model)
        loader.wdb.cache_model_run.assert_called_once_with(loader.api.productinstance['productinstance1'], model)
        loader.wdb2ts.update.assert_called_once_with(loader.api.productinstance['productinstance1'], model)

    def test_multiple_backends(self):
        config = DefaultConfig()
        config.data['model_whatever']['servicebackend'] = 'servicebackend2,servicebackend1'
        loader = TestingDataLoader(config)
        loader._state_database.add_productinstance_to_be_processed(loader.api.productinstance['productinstance1'])

        loader.process()

        model = Any(set)
        loader.wdb.load_model_file.assert_called_once_with(loader.api.datainstance['datainstance2'], model)
        loader.wdb.cache_model_run.assert_called_once_with(loader.api.productinstance['productinstance1'], model)
        loader.wdb2ts.update.assert_called_once_with(loader.api.productinstance['productinstance1'], model)

    def test_first_backend_fails(self):
        config = DefaultConfig()
        config.data['model_whatever']['servicebackend'] = 'servicebackend2,servicebackend1'
        loader = TestingDataLoader(config)
        loader._state_database.add_productinstance_to_be_processed(loader.api.productinstance['productinstance1'])
        call_count = {}

        def load_model_file(datainstance, model):
            if datainstance.id not in call_count:
                call_count[datainstance.id] = 0
            call_count[datainstance.id] += 1
            if datainstance.id == 'datainstance2':
                raise syncer.exceptions.WDBLoadFailed('loading failed')

        loader.wdb.load_model_file.side_effect = load_model_file

        loader.process()

        self.assertEqual(1, call_count['datainstance2'])
        self.assertEqual(1, call_count['datainstance1'])
        model = Any(set)
        loader.wdb.load_model_file.assert_called_with(loader.api.datainstance['datainstance1'], model)  # last call, which succeeded
        loader.wdb.cache_model_run.assert_called_once_with(loader.api.productinstance['productinstance1'], model)
        loader.wdb2ts.update.assert_called_once_with(loader.api.productinstance['productinstance1'], model)

    def test_first_backend_fails_keep_using_backup(self):
        config = DefaultConfig()
        config.data['model_whatever']['servicebackend'] = 'servicebackend2,servicebackend1'
        loader = TestingDataLoader(config)
        loader._state_database.add_productinstance_to_be_processed(loader.api.productinstance['productinstance1'])

        def load_model_file(datainstance, model):
            if datainstance.id == 'datainstance2':
                raise syncer.exceptions.WDBLoadFailed('loading failed')

        loader.wdb.load_model_file.side_effect = load_model_file
        loader.process()
        model = Any(set)
        loader.wdb.load_model_file.assert_called_with(loader.api.datainstance['datainstance1'], model)  # last call, which succeeded
        self.assertEqual(2, loader.wdb.load_model_file.call_count)

        loader.wdb.load_model_file.reset_mock()
        loader._state_database.add_productinstance_to_be_processed(loader.api.productinstance['productinstance1'],
                                                                   even_if_previously_loaded=True)
        loader.process()

        # Loader should have switched to preferring servicebackend1
        loader.wdb.load_model_file.assert_called_with(loader.api.datainstance['datainstance1'], model)
        self.assertEqual(1, loader.wdb.load_model_file.call_count)  # 2 means we first tried datainstance2, which is down
