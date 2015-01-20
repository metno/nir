import falcon
import falcon.testing
import unittest
import json
import modelstatus.api.data
import modelstatus.tests.test_utils

api_base_url = modelstatus.tests.test_utils.get_api_base_url()

class TestDataCollectionResource(falcon.testing.TestBase):
    
    def before(self):

        self.url = api_base_url + '/data'
        self.resource  = modelstatus.api.data.CollectionResource(api_base_url, 
                                                                     modelstatus.tests.test_utils.get_test_logger())
        self.api.add_route(self.url,self.resource)
        self.doc = '{"model_run":"' + api_base_url + '/model_run/1", "format":"netcdf4", "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc"}'


    def test_post_status_code(self):
        body = self.simulate_request(self.url, method='POST', body=self.doc)
        self.assertEqual(self.srmock.status, falcon.HTTP_503)

    def test_get_body(self):
        body = self.simulate_request(self.url,method='GET')

        self.assertEqual(body[0], '[]')

    def test_get_status(self):
        self.simulate_request(self.url,method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

class TestDataItemResource(falcon.testing.TestBase):
    
    def before(self):

        self.base_url = api_base_url + '/data'
        self.route = api_base_url + '/data/{id}'
        self.resource  = modelstatus.api.data.ItemResource(api_base_url,
                                                               modelstatus.tests.test_utils.get_test_logger())
        self.api.add_route(self.route, self.resource)

    def test_get(self):
        self.simulate_request(self.base_url + '/1',method='GET')

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

if __name__ == '__main__':
    unittest.main()
