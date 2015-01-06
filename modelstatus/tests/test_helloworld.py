import falcon
import falcon.testing as testing
import unittest
import helloworld

class TestHelloWorldResource(testing.TestBase):

    def before(self):
        self.url = '/v0/helloworld'
        self.resource = helloworld.HelloWorldResource()
        self.api.add_route(self.url,self.resource)
        
    def test_body(self):
        body = self.simulate_request(self.url)

        self.assertEqual(body, ["Hello world!\n"])

    def test_content_type(self):
        self.simulate_request(self.url)

        self.assertEqual('text/plain', self.srmock.headers_dict['content-type'])

if __name__ == '__main__':
    unittest.main()
