#!/usr/bin/env python2.7

import modelstatus.api
import falcon
import json

def deserialize(req, resp, params ):
    
    body = req.stream.read()
    if not body:
        raise falcon.HTTPBadRequest('Empty request body',
                                    'Include a valid model_run JSON document')
    
    try:
        params['doc'] = json.loads(body.decode('utf-8'))
    except (ValueError, UnicodeDecodeError):
        raise falcon.HTTPError(falcon.HTTP_400,
                               'Malformed JSON',
                               'Could not decode request body. The '
                               'JSON was incorrect or not encoded as UTF-8')


class CollectionResource(modelstatus.api.BaseResource):

    @falcon.before(deserialize)
    def on_post(self, req, resp, doc):

        # Check data, store data. Simply dump request body for now.
        resp.data = json.dumps(doc)
        
        resp.status = falcon.HTTP_201
        resp.location = '/v0/model_run/%d' % (1)
        


