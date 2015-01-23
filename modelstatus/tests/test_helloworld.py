import falcon
import falcon.testing
import unittest
import modelstatus.api.helloworld
import modelstatus.tests.test_utils

api_base_url = modelstatus.tests.test_utils.get_api_base_url()

class TestHelloWorldResource(modelstatus.tests.test_utils.TestBase):

    def before(self):
        self.setup_zmq()
        self.url = api_base_url + '/helloworld'
        self.resource = modelstatus.api.helloworld.HelloWorldResource(api_base_url, 
                                                                      modelstatus.tests.test_utils.get_test_logger(),
                                                                      None,
                                                                      self.zmq)
        self.api.add_route(self.url,self.resource)
        
    def test_body(self):
        body = self.simulate_request(self.url)

        self.assertEqual(body, ["Hello world!\n"])

    def test_content_type(self):
        self.simulate_request(self.url)

        self.assertEqual('text/plain', self.srmock.headers_dict['content-type'])

if __name__ == '__main__':
    unittest.main()
