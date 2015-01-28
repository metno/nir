"""
The REST module defines all classes used to interface against the modelstatus
REST API service.
"""

import requests
import json
import logging
import dateutil.parser

import syncer.exceptions


class BaseResource(object):
    required_parameters = []

    def __init__(self, data):
        """
        Initialize a resource with a Python dictionary.
        The constructor takes a data dictionary instead of a strict parameter
        list because we need to iterate over the parameters anyway and assign
        them to the class. It's simpler and DRY-er just to specify them once,
        in the 'required_parameters' list.
        """
        [setattr(self, key, value) for key, value in data.iteritems()]
        self.validate()
        self.initialize()

    def initialize(self):
        """Do variable initialization, overridden by subclasses"""
        pass

    def validate(self):
        """
        Data validation, run before initialize(). May be overridden by
        subclasses. May throw exceptions.
        """
        try:
            for required_parameter in self.required_parameters:
                getattr(self, required_parameter)
        except:
            raise TypeError("Required parameter %s not specified" % required_parameter)


class ModelRun(BaseResource):
    required_parameters = ['id', 'data_provider', 'reference_time', 'created_date', 'version', 'data']

    def initialize(self):
        self.reference_time = dateutil.parser.parse(self.reference_time)
        self.created_date = dateutil.parser.parse(self.created_date)
        self.data = [Data(x) for x in self.data]

    def __repr__(self):
        return "ModelRun id=%d data_provider=%s reference_time=%s version=%d" % \
            (self.id, self.data_provider, self.reference_time.isoformat(), self.version)


class Data(BaseResource):
    required_parameters = ['id', 'model_run_id', 'format', 'href']

    def __repr__(self):
        return "Data id=%d model_run_id=%d format=%s href=%s" % \
            (self.id, self.model_run_id, self.format, self.href)


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
        response = self.session.get(*args, **kwargs)

        if response.status_code >= 500:
            raise syncer.exceptions.RESTServiceUnavailableException(
                "Server returned error code %d for request uri %s " % (response.status_code, response.request.url))
        elif response.status_code >= 400:
            raise syncer.exceptions.RESTServiceClientErrorException(
                "Server returned error code %d for request %s " % (response.status_code, response.request.url))

        return response

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
        try:
            object_ = resource(data)
        except Exception, e:
            raise syncer.exceptions.InvalidResourceException(e)
        logging.info("Downloaded %s" % object_)
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
        try:
            resources = [resource(x) for x in data]
        except Exception, e:
            raise syncer.exceptions.InvalidResourceException(e)
        return resources


class ModelRunCollection(BaseCollection):
    """Access the 'model_run' collection of the REST service."""

    resource = ModelRun
    resource_name = 'model_run'

    def get_latest(self, data_provider):
        """Returns the latest model run from the specified data_provider."""
        order_by = [
            'reference_time:desc',
            'version:desc',
        ]
        params = {
            'data_provider': data_provider,
            'order_by': ','.join(order_by),
            'limit': 1,
        }
        return self.filter(**params)


class DataCollection(BaseCollection):
    """Access the 'data' collection of the REST service."""

    resource = Data
    resource_name = 'data'
