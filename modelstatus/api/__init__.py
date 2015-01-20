import falcon

import modelstatus.utils

class BaseResource(object):
    """Parent class of all API resources"""

    def __init__(self, api_base_url, logger):
        self.logger = logger
        self.api_base_url = api_base_url


class BaseCollectionResource(BaseResource):
    """Parent class of all collection resources"""

    @falcon.before(modelstatus.utils.deserialize)
    @falcon.after(modelstatus.utils.serialize)
    def on_post(self, req, resp, doc):
        """Create a new resource"""

        # No way to store body for now.
        raise falcon.HTTPError(falcon.HTTP_503,
                               'Unable to create resource',
                               'No backend to store data in!',
                               )

    @falcon.after(modelstatus.utils.serialize)
    def on_get(self, req, resp):
        """List all resources inline"""

        result = []

        resp.body = result
        resp.status = falcon.HTTP_200


class BaseItemResource(BaseResource):
    """Parent class of all item resources"""

    @falcon.after(modelstatus.utils.serialize)
    def on_get(self, req, resp, id):
        """Return a single resource"""

        raise falcon.HTTPError(falcon.HTTP_404,
                               'No such resource exists!'
                               )
