import falcon
import falcon.testing
import unittest
import json
import modelstatus.api.modelrun
import modelstatus.tests.test_utils

api_base_url = modelstatus.tests.test_utils.get_api_base_url()

class TestModelRunCollectionResource(falcon.testing.TestBase):
    
    def before(self):

        self.url = api_base_url + '/model_run'
        self.resource  = modelstatus.api.modelrun.CollectionResource(api_base_url, 
                                                                     modelstatus.tests.test_utils.get_test_logger())
        self.api.add_route(self.url,self.resource)
        self.doc = '{"data_provider": "arome_metcoop_2500m", "reference_time":"2015-01-12T06:00:00Z"}'

    def test_post_status_code(self):
        body  = self.simulate_request(self.url,method='POST', body=self.doc)
        self.assertEqual(self.srmock.status, falcon.HTTP_503)

    def test_get_body(self):
        body = self.simulate_request(self.url,method='GET')

        self.assertEqual(body[0], '[]')

    def test_get_status(self):
        self.simulate_request(self.url,method='GET')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

class TestModelRunItemResource(falcon.testing.TestBase):
    
    def before(self):

        self.base_url = api_base_url + '/model_run'
        self.route = api_base_url + '/model_run/{id}'
        self.resource  = modelstatus.api.modelrun.ItemResource(api_base_url,
                                                               modelstatus.tests.test_utils.get_test_logger())
        self.api.add_route(self.route, self.resource)

    def test_get(self):
        self.simulate_request(self.base_url + '/1',method='GET')

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

if __name__ == '__main__':
    unittest.main()
