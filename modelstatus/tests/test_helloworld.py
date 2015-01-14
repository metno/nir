import falcon
import falcon.testing
import unittest
import modelstatus.api.helloworld

class TestHelloWorldResource(falcon.testing.TestBase):

    def before(self):
        self.url = '/v0/helloworld'
        self.resource = modelstatus.api.helloworld.HelloWorldResource()
        self.api.add_route(self.url,self.resource)
        
    def test_body(self):
        body = self.simulate_request(self.url)

        self.assertEqual(body, ["Hello world!\n"])

    def test_content_type(self):
        self.simulate_request(self.url)

        self.assertEqual('text/plain', self.srmock.headers_dict['content-type'])

if __name__ == '__main__':
    unittest.main()
