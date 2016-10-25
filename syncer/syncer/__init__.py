import logging
import logging.config
import os
import sys
import traceback
import configparser

import syncer.config
import syncer.exceptions
import syncer.loading
import syncer.productstatus_access
import syncer.wdb
import syncer.wdb2ts


DEFAULT_LOG_FILE_PATH = '/var/log/syncer.log'
DEFAULT_LOG_LEVEL = 'DEBUG'
DEFAULT_LOG_FORMAT = '%(asctime)s (%(levelname)s) %(message)s'


def run(config):
    try:
        loader = syncer.loading.DataLoader(config)
        listener = syncer.productstatus_access.Listener(config)

        listener.start()
        listener.new_data.wait()  # Ensure that we have started before proceeding.

        loader.populate_database_with_latest_events_from_server()

        logging.info("Syncer is started")
        try:
            while True:
                listener.new_data.clear()
                loader.process()
                listener.new_data.wait()
        except KeyboardInterrupt:
            listener.stop()

        logging.info('Syncer is stopping')
        listener.join()
    except syncer.exceptions.ConfigurationException as e:
        logging.critical(str(e))
        return syncer.config.EXIT_CONFIG
    return syncer.config.EXIT_SUCCESS


def main(argv):
    # Set up default initial logging, in case something goes wrong during config parsing
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)
    logging.info("Starting Syncer...")

    try:
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

        return run(config)
    except:
        exception = traceback.format_exc().split("\n")
        logging.critical("***********************************************************")
        logging.critical("Uncaught exception during program execution. THIS IS A BUG!")
        logging.critical("***********************************************************")
        for line in exception:
            logging.critical(line)
        return 255

if __name__ == '__main__':
        syncer_root_path = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/..')
        sys.path.append(syncer_root_path)
        exit_code = main(['--config', os.path.join(syncer_root_path, 'etc', 'config.ini')])
        logging.info("Exiting with status %d", exit_code)
        sys.exit(exit_code)
