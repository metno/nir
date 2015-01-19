"""
The REST module defines all classes used to interface against the modelstatus
REST API service.
"""

import requests
import json

import syncer.exceptions


class BaseResource(object):
    def __init__(self, data):
        [setattr(self, key, value) for key, value in data.iteritems()]


class ModelRun(BaseResource):
    pass


class Data(BaseResource):
    pass


class BaseCollection(object):
    """Base object used to access the REST service."""

    def __init__(self, base_url, verify_ssl):
        if not issubclass(self.resource, BaseResource):
            raise TypeError('syncer.rest.BaseCollection.resource must be inherited from syncer.rest.BaseResource')
        self.session = requests.Session()
        self.session.headers.update({'content-type': 'application/json'})
        self.base_url = base_url
        self.verify_ssl = verify_ssl

    def _get_request(self, *args, **kwargs):
        """Wrapper for self.session.get with exception handling"""
        request = self.session.get(*args, **kwargs)
        if request.status_code >= 500:
            raise syncer.exceptions.RESTServiceUnavailableException("Server returned error code %d" % request.status_code)
        elif request.status_code >= 400:
            raise syncer.exceptions.RESTServiceClientErrorException("Server returned error code %d" % request.status_code)
        return request

    def get_collection_url(self):
        """Return the URL for the resource collection"""
        return "%s/%s" % (self.base_url, self.resource_name)

    def get_resource_url(self, id):
        """Given a data set ID, return the web service URL for the data set"""
        return "%s/%d" % (self.get_collection_url(), id)

    def get_request_data(self, request):
        """Get JSON contents from a request object"""
        return request.content

    def unserialize(self, data):
        """Convert JSON encoded data into a dictionary"""
        try:
            return json.loads(data)
        except ValueError, e:
            raise syncer.exceptions.UnserializeException(e)

    def search(self, params):
        """High level function, returns a list of search results"""
        url = self.get_collection_url()
        request = self._get_request(url, params=params)
        data = self.get_request_data(request)
        return self.unserialize(data)

    def get(self, id):
        """High level function, returns a dictionary with resource"""
        url = self.get_resource_url(id)
        request = self._get_request(url)
        data = self.get_request_data(request)
        return self.unserialize(data)

    def get_object(self, id):
        """High level function, returns an object inheriting from BaseResource"""
        data = self.get(id)
        resource = self.resource
        object_ = resource(data)
        return object_

    def filter(self, **kwargs):
        """
        Runs a query against the entire collection, filtering with URL parameters.
        Returns a list of resources inheriting from BaseResource.
        """
        url = self.get_collection_url()
        request = self._get_request(url, params=kwargs, verify=self.verify_ssl)
        data_str = self.get_request_data(request)
        data = self.unserialize(data_str)
        resource = self.resource
        return [resource(x) for x in data]


class ModelRunCollection(BaseCollection):
    """Access the 'model_run' collection of the REST service."""

    resource = ModelRun
    resource_name = 'model_run'

    def get_latest(self, data_provider):
        """Returns the latest model run from the specified data_provider."""
        order_by = [
                'created_time:desc',
                'version:desc',
                ]
        params = {
                'data_provider': data_provider,
                'order_by': ':'.join(order_by),
                'limit': 1,
                }
        return self.filter(**params)


class DataCollection(BaseCollection):
    """Access the 'data' collection of the REST service."""

    resource = Data
    resource_name = 'data'
