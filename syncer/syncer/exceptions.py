"""
This module contains exception objects used in Syncer.
"""

class RESTException(Exception):
    """Thrown when there is an error relating to getting data from the REST API."""
    pass

class RESTServiceClientErrorException(RESTException):
    """Thrown when the server returns a 4xx error."""
    pass

class RESTServiceUnavailableException(RESTException):
    """Thrown when the server returns a 5xx error."""
    pass

class UnserializeException(RESTException):
    """Thrown when the data from the REST API could not be decoded."""
    pass

class InvalidResourceException(RESTException):
    """Thrown when the server returns an invalid resource."""
    pass
