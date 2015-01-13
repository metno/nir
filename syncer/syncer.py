#!/usr/bin/env python2.7

import logging
import sys
import argparse
import ConfigParser

DEFAULT_CONFIG_PATH = '/etc/syncer.ini'
DEFAULT_LOG_FILE_PATH = '/var/log/syncer.log'
DEFAULT_LOG_LEVEL = 'DEBUG'
DEFAULT_LOG_FORMAT = '%(asctime)s (%(levelname)s) %(message)s'

EXIT_SUCCESS = 0
EXIT_CONFIG = 1
EXIT_LOGGING = 2

CONFIG_DEFAULTS = {
    'syncer': {
        'logfile': DEFAULT_LOG_FILE_PATH,
        'loglevel': DEFAULT_LOG_LEVEL,
    }
}

def setup_logging(logfile, loglevel):
    logger = logging.getLogger('')
    logger.setLevel(getattr(logging, loglevel))
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

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

def merge_args_into_config(config_parser, args):
    if args.logfile != CONFIG_DEFAULTS['syncer']['logfile']:
        config_parser.set('syncer', 'logfile', args.logfile)
    if args.logfile != CONFIG_DEFAULTS['syncer']['loglevel']:
        config_parser.set('syncer', 'loglevel', args.loglevel)

def run():
    return EXIT_SUCCESS

def main():

    # Set up default initial logging, in case something goes wrong during config parsing
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)
    logging.info("Starting Syncer")

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
        merge_args_into_config(config, args)
    except IOError, e:
        logging.critical("Could not read configuration file: %s" % unicode(e))
        return EXIT_CONFIG
    except Exception, e:
        logging.critical("Unhandled exception while reading configuration file: %s" % unicode(e))
        raise e

    # Set up proper logging
    try:
        logfile = config.get('syncer', 'logfile')
        loglevel = config.get('syncer', 'loglevel')
        logging.info("Setting log level to %s" % loglevel)
        setup_logging(logfile, loglevel)
        logging.info("Log file opened")
    except IOError, e:
        logging.critical("Could not open log file: %s" % unicode(e))
        return EXIT_LOGGING

    # Start main application
    logging.info("Syncer is ready")
    exitcode = run()
    logging.info("Exiting with status %d", exitcode)

    return exitcode

if __name__ == '__main__':
    sys.exit(main())
