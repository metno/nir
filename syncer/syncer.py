#!/usr/bin/env python2.7

import logging
import logging.config
import sys
import json
import requests
import argparse
import ConfigParser

DEFAULT_CONFIG_PATH = '/etc/syncer.ini'
DEFAULT_LOG_FILE_PATH = '/var/log/syncer.log'
DEFAULT_LOG_LEVEL = 'DEBUG'
DEFAULT_LOG_FORMAT = '%(asctime)s (%(levelname)s) %(message)s'
DEFAULT_WEB_SERVICE_BASE_URL = 'https://modelstatus.met.no'

EXIT_SUCCESS = 0
EXIT_CONFIG = 1
EXIT_LOGGING = 2

CONFIG_DEFAULTS = {
    'syncer': {
        'logfile': DEFAULT_LOG_FILE_PATH,
        'loglevel': DEFAULT_LOG_LEVEL,
    },
    'webservice': {
        'url': DEFAULT_WEB_SERVICE_BASE_URL,
    },
}

#
# Base object used to access the REST service.
#
class BaseCollection(object):
    def __init__(self, base_url, resource_name):
        self.session = requests.Session()
        self.session.headers.update({'content-type': 'application/json'})
        self.base_url = base_url
        self.resource_name = resource_name

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
        return json.loads(data)

    def search(self, params):
        """High level function, returns a list of search results"""
        url = self.get_collection_url()
        request = self.session.get(url, params=params)
        data = self.get_request_data(request)
        return self.unserialize(data)

    def get(self, id):
        """High level function, returns a dictionary with resource"""
        url = self.get_resource_url(id)
        request = self.session.get(url)
        data = self.get_request_data(request)
        return self.unserialize(data)

#
# Access model runs and data sets through the REST service.
#
class ModelRunCollection(BaseCollection):
    def __init__(self, base_url):
        return super(self.__class__, self).__init__(base_url, 'model_run')

class DataCollection(BaseCollection):
    def __init__(self, base_url):
        return super(self.__class__, self).__init__(base_url, 'data')


def setup_logging(config_file):
    """Set up logging based on configuration file."""
    return logging.config.fileConfig(config_file, disable_existing_loggers=True)

def create_config_parser():
    parser = ConfigParser.SafeConfigParser()
    parser.add_section('syncer')
    parser.add_section('wdb')
    return parser

def read_config_file(parser, config_file):
    parser.readfp(config_file)

def create_argument_parser():
    return argparse.ArgumentParser(CONFIG_DEFAULTS, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

def setup_argument_parser(parser):
    parser.add_argument('-c', '--config', help='path to configuration file', default=DEFAULT_CONFIG_PATH)
    parser.add_argument('--logfile', help='path to log file', default=DEFAULT_LOG_FILE_PATH)
    parser.add_argument('--loglevel', help='log level', default=DEFAULT_LOG_LEVEL, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])

def parse_args(parser, args):
    return parser.parse_args(args)

def run(config):
    # Sample usage:
    #base_url = config.get('webservice', 'url')
    #model_run_store = ModelRunCollection(base_url)
    #model_run = model_run_store.get(1)
    return EXIT_SUCCESS

def main():

    # Parse command line arguments
    try:
        argument_parser = create_argument_parser()
        setup_argument_parser(argument_parser)
        args = parse_args(argument_parser, sys.argv[1:])
    except Exception, e:
        logging.critical("Unhandled exception while parsing command line options: %s" % unicode(e))
        raise e

    # Read the configuration file
    try:
        config = create_config_parser()
        read_config_file(config, open(args.config))
    except IOError, e:
        logging.critical("Could not read configuration file: %s" % unicode(e))
        return EXIT_CONFIG
    except Exception, e:
        logging.critical("Unhandled exception while reading configuration file: %s" % unicode(e))
        raise e

    # Set up proper logging
    try:
        setup_logging(args.config)
    except ConfigParser.Error, e:
        logging.critical("There is an error in the logging configuration: %s" % unicode(e))
        return EXIT_LOGGING
    except IOError, e:
        logging.critical("Could not read logging configuration file: %s" % unicode(e))
        return EXIT_LOGGING

    # Start main application
    logging.info("Syncer is started")
    exitcode = run(config)

    return exitcode

if __name__ == '__main__':

    # Set up default initial logging, in case something goes wrong during config parsing
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)
    logging.info("Starting Syncer...")

    exit_code = main()

    logging.info("Exiting with status %d", exit_code)
    sys.exit(exit_code)
