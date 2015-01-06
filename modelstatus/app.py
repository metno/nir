import falcon
import helloworld
from wsgiref import simple_server

app = falcon.API()
helloworld = helloworld.HelloWorldResource()

app.add_route('/v0/helloworld', helloworld)

if __name__ == '__main__':
    httpd = simple_server.make_server('127.0.0.1', 8000, app)
    httpd.serve_forever()
