import falcon
import falcon.testing
import unittest
import json

import modelstatus.orm
import modelstatus.api.data
import modelstatus.tests.test_utils


class TestDataCollectionResource(falcon.testing.TestBase):

    def before(self):

        self.api_base_url = modelstatus.tests.test_utils.get_api_base_url()
        self.url = self.api_base_url + '/data'
        self.orm = modelstatus.orm.get_sqlite_memory_session()
        self.resource  = modelstatus.api.data.CollectionResource(self.api_base_url,
                                                                 modelstatus.tests.test_utils.get_test_logger(),
                                                                 modelstatus.orm.Data())
        self.api.add_route(self.url,self.resource)
        self.doc = '{"model_run":"' + self.api_base_url + '/model_run/1", "format":"netcdf4", "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc"}'

    def test_post_status_code(self):
        body = self.simulate_request(self.url, method='POST', body=self.doc)
        self.assertEqual(self.srmock.status, falcon.HTTP_503)

    def test_get_body(self):
        body = self.simulate_request(self.url, method='GET')
        self.assertEqual(body[0], '[]')

    def test_get_status(self):
        self.simulate_request(self.url, method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)


class TestDataItemResource(falcon.testing.TestBase):

    def before(self):

        self.api_base_url = modelstatus.tests.test_utils.get_api_base_url()
        self.url = self.api_base_url + '/data'
        self.orm = modelstatus.orm.get_sqlite_memory_session()
        self.route = self.api_base_url + '/data/{id}'
        self.resource  = modelstatus.api.data.ItemResource(self.api_base_url,
                                                           modelstatus.tests.test_utils.get_test_logger(),
                                                           modelstatus.orm.Data())
        self.api.add_route(self.route, self.resource)

    def test_get(self):
        self.simulate_request(self.url + '/1', method='GET')

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

if __name__ == '__main__':
    unittest.main()
