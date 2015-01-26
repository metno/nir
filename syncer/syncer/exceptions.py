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

class WDBLoadException(Exception):
    """Thrown when a load program failed to load model data into wdb."""
    pass

class OpdataURIException(Exception):
    """Thrown when the uri given is not a correct opdata uri."""
    pass
