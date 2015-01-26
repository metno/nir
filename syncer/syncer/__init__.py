# coding: utf-8

import logging
import logging.config
import sys
import requests
import argparse
import ConfigParser
import time
import re
import subprocess

import syncer.rest

DEFAULT_CONFIG_PATH = '/etc/syncer.ini'
DEFAULT_LOG_FILE_PATH = '/var/log/syncer.log'
DEFAULT_LOG_LEVEL = 'DEBUG'
DEFAULT_LOG_FORMAT = '%(asctime)s (%(levelname)s) %(message)s'

EXIT_SUCCESS = 0
EXIT_CONFIG = 1
EXIT_LOGGING = 2


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

    def section_keys(self, section_name):
        return [x[0] for x in self.config_parser.items(section_name)]

    def section_options(self, section_name):
        return dict(self.config_parser.items(section_name))

class WDB:

    def __init__(self, host, user):
        self.host = host
        self.user = user
        

    def load_model_run(self, model, model_run):
        """Load a model_run."""

        data_uri_pattern = model.data_uri_pattern

        for data in model_run.data:              
            data_uri = data.href
            
            if re.search(data_uri_pattern, data_uri) is not None:
                
                modelfile = WDB.convert_opdata_uri_to_file(data_uri)
                self.load_modelfile(model, modelfile)


    def load_modelfile(self, model, modelfile):
        """Load a modelfile into wdb host"""

        load_cmd = WDB.create_load_command(model, modelfile)
        cmd = self.create_ssh_command(load_cmd)

        try:
            exit_code, stdout, stderr = WDB.execute_command(cmd)

        except subprocess.CalledProcessError, e:
            raise syncer.exceptions.WDBLoadException("Load command not found: %s" % e)

    @staticmethod
    def execute_command(cmd):      

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        res = process.communicate()
        exit_code = process.wait()

        return exit_code, res[0], res[1]

    @staticmethod
    def create_load_command(model, modelfile):

        cmd = [model.load_program, '--dataprovider', model.data_provider]

        if hasattr(model, 'load_config'):
            cmd.extend(['-c', model.load_config])
 
        if hasattr(model, 'place_name'):
            cmd.extend(["--placename", model.place_name])
        else:
            cmd.extend(["--loadPlaceDefinition"])
        
        cmd.append(modelfile)
        
        return cmd

    def create_ssh_command(self, cmd):
        return ["ssh", "{0}@{1}".format(self.user, self.host)] + cmd

    @staticmethod
    def convert_opdata_uri_to_file(data_uri):
        """Convert an opdata uri to a file name with full path."""

        # uri must start with opdata:/// ( and max 3 '/' )
        if re.match('opdata\:\/{3}(?!\/)', data_uri) is None:
            raise syncer.exceptions.OpdataURIException(
                "The uri {0} is not correctly formatted".format(data_uri)) 

        data_file_path = re.sub(r'^opdata\:\/\/\/', '/opdata/', data_uri)

        return data_file_path


class Model:
    def __init__(self, data):
        [setattr(self, key, value) for key, value in data.iteritems()]
        self.current_model_run = None
        self.current_model_run_initialized = False

    @staticmethod
    def data_from_config_section(config, section_name):
        """Return config options for a model. Raise exception if mandatory config option is missing"""
      
        data = {}
        mandatory_options = ['data_provider', 'data_uri_pattern', 'load_program', 'load_config']

        section_keys = config.section_keys(section_name)
        for option in mandatory_options:
            if not option in section_keys:
                raise ConfigParser.NoOptionError(option, section_name)

        data = config.section_options(section_name)

        return data

    def set_current_model_run(self, model_run):
        if model_run is not None and not isinstance(model_run, syncer.rest.BaseResource):
            raise TypeError("%s argument 'model_run' must inherit from syncer.rest.BaseResource" % __func__)
        self.current_model_run = model_run
        self.current_model_run_initialized = True

    def __repr__(self):
        return self.data_provider


class Daemon:
    def __init__(self, config, models, model_run_collection, wdb):
        self.config = config
        self.models = models
        self.model_run_collection = model_run_collection
        self.wdb = wdb

        if not isinstance(models, set):
            raise TypeError("'models' must be a set of models")
        for model in self.models:
            if not isinstance(model, Model):
                raise TypeError("'models' set must contain only models")

        logging.info("Daemon initialized with the following model configuration:")
        num_models = len(self.models)
        for num, model in enumerate(self.models):
            logging.info(" %2d of %2d: %s" % (num_models, num+1, model.data_provider))


    def get_latest_model_run(self, model):
        """Fetch the latest model run from REST API, and assign it to the provided Model."""

        if not isinstance(model, Model):
            raise TypeError("Only accepts syncer.Model as argument")

        try:
            # Try fetching the latest data set
            latest = self.model_run_collection.get_latest(model.data_provider)

            # No results from server, should only happen in freshly installed instances
            if len(latest) == 0:
                logging.info("REST API does not contain any recorded model runs.")
                logging.warn("Syncer will not query for model runs again until restarted, or notified by publisher.")
                model.set_current_model_run(None)

            # More than one result, this is a server error and should not happen
            elif len(latest) > 1:
                logging.error("REST API returned more than one result when fetching latest model run, this should not happen!")

            # Valid result
            else:
                model.set_current_model_run(latest[0])
                logging.info("Model %s has new model run: %s" % (model, model.current_model_run))

        # Server threw an error, recover from that
        except syncer.exceptions.RESTException, e:
            logging.error("REST API threw up with an exception: %s" % e)

        
    def load_model(self, model):

        try:
            self.wdb.load_model_run(model, model.current_model_run)
                                                                
        except syncer.exceptions.WDBLoadException, e:
            logging.error("Failed to load model into wdb: %s" % e)              
        except syncer.exceptions.OpdataURIException, e:
            logging.critical("Failed to load some model data due to erroneous opdata uri: %s" % e)
                              

    def main_loop_inner(self):
        """Workhorse of the main loop"""
        for model in self.models:

            # Try to initialize all un-initialized models with current model run status
            if not model.current_model_run_initialized:
                logging.info("Model %s does not have any information about model runs, initializing from API..." % model)
                self.get_latest_model_run(model)
                self.load_model(model)
                
                
    def run(self):
        """Responsible for running the main loop. Returns the program exit code."""
        logging.info("Daemon started.")

        try:
            while True:
                self.main_loop_inner()
                time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Terminated by SIGINT")

        logging.info("Daemon is terminating.")
        return EXIT_SUCCESS




def setup_logging(config_file):
    """Set up logging based on configuration file."""
    return logging.config.fileConfig(config_file, disable_existing_loggers=True)


def run(argv):

    # Parse command line arguments and read the configuration file
    try:
        config = Configuration()
        config.parse_args(argv)
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

    try:
        wdb = WDB(config.get('wdb','host'), config.get('wdb','ssh_user'))
    except ConfigParser.NoOptionError, e:
        logging.critical("Missing configuration for wdb host")
        return EXIT_CONFIG
        
    # Start main application
    logging.info("Syncer is started")
    model_keys = set([model.strip() for model in config.get('syncer', 'models').split(',')])
    models = set([Model(Model.data_from_config_section(config, 'model_%s' % key)) for key in model_keys])
    base_url = config.get('webservice', 'url')
    verify_ssl = bool(int(config.get('webservice', 'verify_ssl')))
    model_run_collection = syncer.rest.ModelRunCollection(base_url, verify_ssl)

    daemon = Daemon(config, models, model_run_collection, wdb)
    exit_code = daemon.run()

    return exit_code


def main(argv):
    # Set up default initial logging, in case something goes wrong during config parsing
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)
    logging.info("Starting Syncer...")

    exit_code = run(argv)

    logging.info("Exiting with status %d", exit_code)
    sys.exit(exit_code)
