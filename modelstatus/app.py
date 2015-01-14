
import falcon
import modelstatus.api.helloworld 
import modelstatus.api.modelrun

from wsgiref import simple_server

app = falcon.API()
helloworld = modelstatus.api.helloworld.HelloWorldResource()
modelrun_collection = modelstatus.api.modelrun.CollectionResource()

app.add_route('/v0/helloworld', helloworld)
app.add_route('/v0/model_run', modelrun_collection)

if __name__ == '__main__':
    httpd = simple_server.make_server('0.0.0.0', 8000, app)
    httpd.serve_forever()
