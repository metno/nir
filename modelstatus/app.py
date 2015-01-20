#!/usr/bin/env python2.7

import sys
import falcon
import ConfigParser
import argparse
import logging
import logging.config

import modelstatus.orm
import modelstatus.api.helloworld 
import modelstatus.api.modelrun
import modelstatus.api.data

from wsgiref import simple_server

DEFAULT_CONFIG_PATH = '/etc/modelstatus.ini'
DEFAULT_LOG_LEVEL = 'DEBUG'
DEFAULT_LOG_FORMAT  = '%(asctime)s (%(levelname)s) %(message)s'

EXIT_CONFIG = 1
EXIT_LOGGING = 2

def setup_logger(config_file):
    logging.config.fileConfig(config_file, disable_existing_loggers=True)

    return logging.getLogger()

def parse_arguments(args):

    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-c', '--config', help="path to config file", 
                               default=DEFAULT_CONFIG_PATH)

    return args_parser.parse_args(args)


def start_api(logger):
    """Instantiate api, add all resources and routes and return application object."""

    api_base_url = '/modelstatus/v0'
    application = falcon.API()

    helloworld = modelstatus.api.helloworld.HelloWorldResource(api_base_url, logger, None)

    modelrun_orm = modelstatus.orm.ModelRun()
    modelrun_collection = modelstatus.api.modelrun.CollectionResource(api_base_url, logger, modelrun_orm)
    modelrun_item = modelstatus.api.modelrun.ItemResource(api_base_url, logger, modelrun_orm)

    data_orm = modelstatus.orm.Data()
    data_collection = modelstatus.api.data.CollectionResource(api_base_url, logger, data_orm)
    data_item = modelstatus.api.data.ItemResource(api_base_url, logger, data_orm)

    
    application.add_route(api_base_url + '/helloworld', helloworld)
    application.add_route(api_base_url + '/model_run', modelrun_collection)
    application.add_route(api_base_url + '/model_run/{id}', modelrun_item)
    application.add_route(api_base_url + '/data', data_collection)
    application.add_route(api_base_url + '/data/{id}', data_item)

    return application


def main():    
    """Set up default initial logging, in case something goes wrong during config parsing."""

    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)

    args = parse_arguments(sys.argv[1:])
    config_file = args.config

    try:
        logger = setup_logger(config_file)

    except (ConfigParser.NoSectionError, IOError) as e:
        logging.critical("There is an error in the logging configuration: %s" 
                         % unicode(e))
        sys.exit(EXIT_LOGGING) 

    return start_api(logger)

if __name__ == '__main__':

    app = main()
    httpd = simple_server.make_server('0.0.0.0', 8000, app)
    httpd.serve_forever()
