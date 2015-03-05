#!/usr/bin/env python2.7

import sys
import falcon
import ConfigParser
import argparse
import logging
import logging.config

import modelstatus.orm
import modelstatus.zeromq
import modelstatus.api.helloworld
import modelstatus.api.modelrun
import modelstatus.api.data

DEFAULT_CONFIG_PATH = '/etc/modelstatus.ini'
DEFAULT_LOG_LEVEL = 'DEBUG'
DEFAULT_LOG_FORMAT = '%(asctime)s (%(levelname)s) %(message)s'

EXIT_CONFIG = 1
EXIT_LOGGING = 2


def setup_logger(config_file):
    logging.config.fileConfig(config_file, disable_existing_loggers=True)


def parse_arguments(args):
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-c', '--config', help="path to config file", default=DEFAULT_CONFIG_PATH)
    return args_parser.parse_args(args)


def start_api(config_parser):
    """Instantiate api, add all resources and routes and return application object."""

    zmq_socket = config_parser.get('zeromq', 'socket')
    database_uri = config_parser.get('database', 'uri')
    api_base_url = '/modelstatus/v0'
    application = falcon.API()

    # connect to database
    logging.info("Connecting to database backend...")
    orm_session = modelstatus.orm.get_database_session(database_uri)

    # instantiate ZeroMQ publisher
    logging.info("Publishing ZeroMQ events on socket %s" % zmq_socket)
    zeromq = modelstatus.zeromq.ZMQPublisher(zmq_socket)

    # instantiate resources - the API end point, where the application logic happens
    common_args = (api_base_url, orm_session, zeromq)
    helloworld = modelstatus.api.helloworld.HelloWorldResource(*common_args)
    modelrun_collection = modelstatus.api.modelrun.CollectionResource(*common_args)
    modelrun_item = modelstatus.api.modelrun.ItemResource(*common_args)
    data_collection = modelstatus.api.data.CollectionResource(*common_args)
    data_item = modelstatus.api.data.ItemResource(*common_args)

    # set up routes
    application.add_route(api_base_url + '/helloworld', helloworld)
    application.add_route(api_base_url + '/model_run', modelrun_collection)
    application.add_route(api_base_url + '/model_run/{id}', modelrun_item)
    application.add_route(api_base_url + '/data', data_collection)
    application.add_route(api_base_url + '/data/{id}', data_item)

    # WSGI application object
    logging.info("Modelstatus startup complete, ready to serve requests.")
    return application


def main(arguments):
    """Set up default initial logging, in case something goes wrong during config parsing."""

    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)

    args = parse_arguments(arguments)
    config_file = args.config

    try:
        setup_logger(config_file)

    except (ConfigParser.NoSectionError, IOError) as e:
        logging.critical("There is an error in the logging configuration: %s"
                         % unicode(e))
        sys.exit(EXIT_LOGGING)

    logging.info("Modelstatus REST API server starting.")

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(config_file)

    logging.info("Configuration file has been successfully read.")

    return start_api(config_parser)
