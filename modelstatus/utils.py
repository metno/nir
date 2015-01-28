"""Various utility functions"""

import json
import falcon


def deserialize(req, resp, params):
    """Convert a JSON string into a Python dictionary"""

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


def serialize(req, resp):
    """Convert a Python dictionary into a JSON string"""
    if resp.body is not None:
        resp.body = json.dumps(resp.body)
