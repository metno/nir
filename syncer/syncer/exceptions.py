"""
This module contains exception objects used in syncer.
"""


class ConfigurationException(Exception):
    pass


class RESTException(Exception):
    """Thrown when there is an error relating to getting data from the REST API."""
    pass


class MissingKafkaHeartbeat(Exception):
    """Thrown when we haven't received a heartbeat message from kafka in a long time."""
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


class MissingStateFile(Exception):
    """Thrown when not able to find a referenced state file"""
    pass


class WDBAccessException(Exception):
    """Any error when accessing wdb database"""
    pass


class WDBLoadFailed(WDBAccessException):
    """Thrown when a load program failed to load model data into wdb."""
    pass


class WDBCacheFailed(WDBAccessException):
    """Thrown when WDB can't cache data."""
    pass


class OpdataURIException(Exception):
    """Thrown when the uri given is not a correct opdata uri."""
    pass


class WDB2TSException(Exception):
    """Base class for WDB2TS errors."""
    pass


class WDB2TSClientException(WDB2TSException):
    """Base class for client errors during WDB2TS calls."""
    pass


class WDB2TSServerException(WDB2TSException):
    """Base class for client errors during WDB2TS calls."""
    pass


class WDB2TSMissingContentException(WDB2TSServerException):
    """Thrown when wdb2ts is missing information in reply."""
    pass


class WDB2TSServiceUnavailableException(WDB2TSServerException):
    """Thrown when wdb2ts returns a 5xx error."""
    pass


class WDB2TSServiceClientErrorException(WDB2TSClientException):
    """Thrown when wdb2ts returns a 4xx error."""
    pass


class WDB2TSWrongContentException(WDB2TSServerException):
    """Thrown when a response from wdb2ts returns a content that is not correct"""
    pass


class WDB2TSConnectionFailure(WDB2TSServerException):
    """Thrown when a request to wdb2ts failed for some reason"""
    pass


class WDB2TSClientUpdateFailure(WDB2TSClientException):
    """Thrown when an update request to wdb2ts failed"""
    pass


class WDB2TSServerUpdateFailure(WDB2TSServerException):
    """Thrown when an update request to wdb2ts failed"""
    pass
