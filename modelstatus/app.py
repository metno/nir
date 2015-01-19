
import sys
import falcon
import ConfigParser
import argparse
import logging
import logging.config
import modelstatus.api.helloworld 
import modelstatus.api.modelrun

from wsgiref import simple_server

DEFAULT_CONFIG_PATH = '/etc/config.ini'
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

    application = falcon.API()
    helloworld = modelstatus.api.helloworld.HelloWorldResource(logger)
    modelrun_collection = modelstatus.api.modelrun.CollectionResource(logger)

    application.add_route('/v0/helloworld', helloworld)
    application.add_route('/v0/model_run', modelrun_collection)

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
