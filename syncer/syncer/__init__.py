import logging
import logging.config
import os
import sys
import traceback
import configparser

import syncer.config
import syncer.exceptions
import syncer.daemon
import syncer.wdb
import syncer.wdb2ts


DEFAULT_LOG_FILE_PATH = '/var/log/syncer.log'
DEFAULT_LOG_LEVEL = 'DEBUG'
DEFAULT_LOG_FORMAT = '%(asctime)s (%(levelname)s) %(message)s'


def run(config):
    try:
        daemon = syncer.daemon.Daemon(config)
        daemon.run()
    except syncer.exceptions.ConfigurationException as e:
        logging.critical(str(e))
        return syncer.config.EXIT_CONFIG
    return syncer.config.EXIT_SUCCESS


def main(argv):
    # Set up default initial logging, in case something goes wrong during config parsing
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)
    logging.info("Starting Syncer...")

    config = syncer.config.get_config(argv)

    # Set up proper logging
    try:
        logging.config.fileConfig(config.args.config, disable_existing_loggers=True)
    except configparser.Error as e:
        logging.critical("There is an error in the logging configuration: %s" % str(e))
        return syncer.config.EXIT_LOGGING
    except IOError as e:
        logging.critical("Could not read logging configuration file: %s" % str(e))
        return syncer.config.EXIT_LOGGING

    exit_code = run(config)

    return exit_code


if __name__ == '__main__':
    try:
        syncer_root_path = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/..')
        sys.path.append(syncer_root_path)
        exit_code = main(['--config', os.path.join(syncer_root_path, 'etc', 'config.ini')])
        logging.info("Exiting with status %d", exit_code)
        sys.exit(exit_code)
    except:
        exception = traceback.format_exc().split("\n")
        logging.critical("***********************************************************")
        logging.critical("Uncaught exception during program execution. THIS IS A BUG!")
        logging.critical("***********************************************************")
        for line in exception:
            logging.critical(line)
        sys.exit(255)
