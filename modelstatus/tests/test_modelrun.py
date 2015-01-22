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
        """
        Test that the API throws error 400 when an 'id' parameter is specified
        while making a POST request.
        """
        self.doc['id'] = 42
        body = self.simulate_request(self.url, method='POST', body=json.dumps(self.doc))
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

    def test_post(self):
        """
        Test that a POST request generates a 201 status code and that a JSON
        body is returned with the created object.
        """
        body = self.simulate_request(self.url, method='POST', body=json.dumps(self.doc))
        self.assertEqual(self.srmock.status, falcon.HTTP_201)
        body_content = self.decode_body(body)
        self.assertEqual(body_content['reference_time'], "2015-01-12T06:00:00+00:00")

    def test_correct_time_zone_conversion(self):
        """
        Test that arbitrary time zones are correctly stored and sent as UTC.
        """
        self.doc['reference_time'] = '2015-01-12T06:00:00+09:00'
        body = self.simulate_request(self.url, method='POST', body=json.dumps(self.doc))
        self.assertEqual(self.srmock.status, falcon.HTTP_201)
        body_content = self.decode_body(body)
        self.assertEqual(body_content['reference_time'], "2015-01-11T21:00:00+00:00")

    def test_get(self):
        """
        Test that a GET request returns an array with resources.
        """
        body = self.simulate_request(self.url, method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        body_content = self.decode_body(body)
        self.assertEqual(len(body_content), 2)
        self.assertEqual(body_content[0]['id'], 1)

    def test_get_full_filters(self):
        """
        Test that filtering by data_provider and reference_time, in addition to
        using order_by and limit, returns one result.
        """
        query_string = "?data_provider=arome25&reference_time=1970-01-01T00:00:00Z&order_by=version:desc&limit=1"
        body = self.simulate_request(self.url, method='GET', query_string=query_string)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        body_content = self.decode_body(body)
        self.assertEqual(len(body_content), 1)
        self.assertEqual(body_content[0]['id'], 1)

    def test_get_negative_limit(self):
        """
        Test that the limit parameter can not be negative.
        """
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
        """
        Test that a GET request returns a resource.
        """
        body = self.simulate_request(self.url + '/1', method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        body_content = self.decode_body(body)
        self.assertEqual(body_content['id'], 1)

if __name__ == '__main__':
    unittest.main()
