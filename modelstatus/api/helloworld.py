import falcon
import modelstatus.api

class HelloWorldResource(modelstatus.api.BaseResource):

    def on_get(self, req, resp):
        resp.body = "Hello world!\n"
        resp.content_type = 'text/plain'
        
        resp.status = falcon.HTTP_200
        
