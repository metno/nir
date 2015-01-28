import dateutil.parser
import falcon
import falcon.testing
import unittest
import json

import modelstatus.orm
import modelstatus.api.data
import modelstatus.tests.test_utils


class TestDataCollectionResource(modelstatus.tests.test_utils.TestBase):

    def before(self):

        self.setup_zmq()
        self.api_base_url = modelstatus.tests.test_utils.get_api_base_url()
        self.url = self.api_base_url + '/data'
        self.orm = modelstatus.orm.get_sqlite_memory_session()
        self.resource = modelstatus.api.data.CollectionResource(self.api_base_url,
                                                                modelstatus.tests.test_utils.get_test_logger(),
                                                                self.orm,
                                                                self.zmq)
        self.setup_database_fixture()
        self.api.add_route(self.url, self.resource)
        self.doc = json.dumps({
            "model_run_id": 1,
            "format": "netcdf4",
            "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc"
        })

    def test_post_missing_data(self):
        self.simulate_request(self.url, method='POST')
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

    def test_post_status_code_created(self):
        self.simulate_request(self.url, method='POST', body=self.doc)
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

    def test_get_body(self):
        body = self.simulate_request(self.url, method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        body_content = self.decode_body(body)
        self.assertEqual(len(body_content), 8)
        self.assertEqual(body_content[0]['model_run_id'], 1)
        self.assertEqual(body_content[0]['href'], "/dev/null")
        self.assertEqual(body_content[0]['id'], 1)
        self.assertEqual(body_content[0]['format'], "netcdf4")
        try:
            dateutil.parser.parse(body_content[0]['created_time'])
        except ValueError:
            self.fail("created_time does not parse as a datetime object")
        self.assertEqual(body_content[1]['model_run_id'], 1)
        self.assertEqual(body_content[1]['href'], "/dev/null")
        self.assertEqual(body_content[1]['id'], 2)
        self.assertEqual(body_content[1]['format'], "netcdf4")

        try:
            dateutil.parser.parse(body_content[1]['created_time'])
        except ValueError:
            self.fail("created_time does not parse as a datetime object")

    def test_get_status(self):
        self.simulate_request(self.url, method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

    def test_post_invalid_model_run_id(self):
        """
        It should not be possible to add data resources associated
        with a non-existing model_run.
        """
        doc = json.dumps({
            "model_run_id": 9999,  # does not exist
            "format": "netcdf4",
            "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc"
        })
        self.simulate_request(self.url, method='POST', body=doc)
        self.assertEqual(self.srmock.status, falcon.HTTP_400)


class TestDataItemResource(modelstatus.tests.test_utils.TestBase):

    def before(self):

        self.setup_zmq()
        self.api_base_url = modelstatus.tests.test_utils.get_api_base_url()
        self.url = self.api_base_url + '/data'
        self.orm = modelstatus.orm.get_sqlite_memory_session()
        self.route = self.api_base_url + '/data/{id}'
        self.resource = modelstatus.api.data.ItemResource(self.api_base_url,
                                                          modelstatus.tests.test_utils.get_test_logger(),
                                                          self.orm,
                                                          self.zmq)
        self.setup_database_fixture()
        self.api.add_route(self.route, self.resource)

    def test_get(self):
        self.simulate_request(self.url + '/1', method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)


if __name__ == '__main__':
    unittest.main()
