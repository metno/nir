import datetime
import falcon
import falcon.testing
import unittest
import json

import modelstatus.orm
import modelstatus.api.modelrun
import modelstatus.tests.test_utils


class TestModelRunCollectionResource(modelstatus.tests.test_utils.TestBase):

    def before(self):

        self.api_base_url = modelstatus.tests.test_utils.get_api_base_url()
        self.url = self.api_base_url + '/model_run'
        self.orm = modelstatus.orm.get_sqlite_memory_session()
        self.resource = modelstatus.api.modelrun.CollectionResource(self.api_base_url,
                                                                    modelstatus.tests.test_utils.get_test_logger(),
                                                                    self.orm)
        self.setup_database_fixture()
        self.api.add_route(self.url, self.resource)
        self.doc = {"data_provider": "arome_metcoop_2500m", "reference_time": "2015-01-12T06:00:00Z"}

    def test_post_fails_on_id(self):
        self.doc['id'] = 42
        body = self.simulate_request(self.url, method='POST', body=json.dumps(self.doc))
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

    def test_post_status_code(self):
        body = self.simulate_request(self.url, method='POST', body=json.dumps(self.doc))
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

    def test_get_body(self):
        body = self.simulate_request(self.url, method='GET')
        body_content = json.loads(body[0])
        self.assertEqual(len(body_content), 2)

    def test_get_status(self):
        self.simulate_request(self.url, method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

    def test_get_full_filters(self):
        query_string = "?data_provider=arome25&reference_time=1970-01-01T00:00:00Z&order_by=version:desc&limit=1"
        body = self.simulate_request(self.url, method='GET', query_string=query_string)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        body_content = json.loads(body[0])
        self.assertEqual(len(body_content), 1)

    def test_get_negative_limit(self):
        query_string = "?limit=-1"
        self.simulate_request(self.url, method='GET', query_string=query_string)
        self.assertEqual(self.srmock.status, falcon.HTTP_400)


class TestModelRunItemResource(modelstatus.tests.test_utils.TestBase):

    def before(self):

        self.api_base_url = modelstatus.tests.test_utils.get_api_base_url()
        self.url = self.api_base_url + '/model_run'
        self.route = self.api_base_url + '/model_run/{id}'
        self.orm = modelstatus.orm.get_sqlite_memory_session()
        self.resource = modelstatus.api.modelrun.ItemResource(self.api_base_url,
                                                              modelstatus.tests.test_utils.get_test_logger(),
                                                              self.orm)
        self.setup_database_fixture()
        self.api.add_route(self.route, self.resource)

    def test_get(self):
        self.simulate_request(self.url + '/1', method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

if __name__ == '__main__':
    unittest.main()
