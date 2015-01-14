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


class Configuration:
    def __init__(self, *args, **kwargs):
        self.config_parser = kwargs['config_parser'] if 'config_parser' in kwargs else self.create_config_parser()
        self.argument_parser = kwargs['argument_parser'] if 'argument_parser' in kwargs else self.create_argument_parser()
        self.setup_config_parser()
        self.setup_argument_parser()
        self.args = object

    def load(self, config_file):
        """Read a configuration file"""
        self.config_parser.readfp(config_file)

    @staticmethod
    def create_config_parser():
        """Instantiate a configuration parser"""
        return ConfigParser.SafeConfigParser()

    @staticmethod
    def create_argument_parser():
        """Instantiate a command line argument parser"""
        return argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    def setup_config_parser(self):
        self.config_parser.add_section('syncer')
        self.config_parser.add_section('wdb')

    def setup_argument_parser(self):
        self.argument_parser.add_argument('-c', '--config', help='path to configuration file', default=DEFAULT_CONFIG_PATH)

    def parse_args(self, args):
        self.args = self.argument_parser.parse_args(args)

    def get(self, section, key):
        return self.config_parser.get(section, key)


def setup_logging(config_file):
    """Set up logging based on configuration file."""
    return logging.config.fileConfig(config_file, disable_existing_loggers=True)


def run(config):
    # Sample usage:
    #base_url = config.get('webservice', 'url')
    #model_run_store = ModelRunCollection(base_url)
    #model_run = model_run_store.get(1)
    return EXIT_SUCCESS


def main():

    # Parse command line arguments and read the configuration file
    try:
        config = Configuration()
        config.parse_args(sys.argv[1:])
        config.load(open(config.args.config))
    except IOError, e:
        logging.critical("Could not read configuration file: %s" % unicode(e))
        return EXIT_CONFIG
    except Exception, e:
        logging.critical("Unhandled exception while loading configuration: %s" % unicode(e))
        raise e

    # Set up proper logging
    try:
        setup_logging(config.args.config)
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
