import falcon
import falcon.testing
import unittest
import modelstatus.api.modelrun
import json

class TestModelRunResource(falcon.testing.TestBase):
    
    def before(self):
        self.url = '/v0/model_run'
        self.resource  = modelstatus.api.modelrun.CollectionResource()
        self.api.add_route(self.url,self.resource)
        self.doc = json.dumps('{"data_provider": "arome_metcoop_2500m", "reference_time":"2015-01-12T06:00:00Z"}')

    def test_post_status_code(self):
        self.simulate_request(self.url,method='POST', body=self.doc)
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

    def test_post_location(self):
        self.simulate_request(self.url,method='POST', body=self.doc)
        self.assertEqual(self.srmock.headers_dict['location'], self.url + '/1')


if __name__ == '__main__':
    unittest.main()
