# coding: utf-8

import logging
import logging.config
import sys
import argparse
import ConfigParser
import time
import re
import subprocess
import lxml.etree
import requests

import syncer.rest
import syncer.zeromq

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


class WDB2TS(object):

    def __init__(self, base_url, services):
        self.base_url = base_url
        self.session = requests.Session()
        self.status = dict.fromkeys(services, {})

    def request_status(self, service):
        """
        Request wdb2ts host for status for specified service and return xml.
        """
        status_url = "%s/%s?status" % (self.base_url, service)
        status_xml = self._get_request(status_url)

        # Validate xml
        try:
            tree = lxml.etree.fromstring(status_xml)
        except lxml.etree.XMLSyntaxError, e:
            raise syncer.exceptions.WDB2TSMissingContentException("Could not parse xml content from request %: %s."
                                                                  % (status_url, e))
        else:
            if not tree.xpath('boolean(/status)'):
                raise syncer.exceptions.WDB2TSMissingContentException(
                    "Content from status request %s is missing its /status element." % status_url)

        return status_xml

    def _get_request(self, url):
        """
        Wrapper for self.session.get with exception handling. Returns body of response.
        """
        try:
            response = self.session.get(url)
        except requests.ConnectionError, e:
            raise syncer.exceptions.WDB2TSRequestFailedException("WDB2TS request %s got connection refused: %s"
                                                                 % (url, e))

        if response.status_code >= 500:
            raise syncer.exceptions.WDB2TSServiceUnavailableException(
                "WDB2TS returned error code %d for request uri %s " % (response.status_code, response.request.url))
        elif response.status_code >= 400:
            raise syncer.exceptions.WDB2TSServiceClientErrorException(
                "WDB2TS returned error code %d for request %s " % (response.status_code, response.request.url))

        return response.content

    def load_status(self):
        """
        Set status dict for all defined services.
        """
        for service in self.status.keys():
            logging.info("Load status information from wdb2ts %s for service %s." %
                         (self.base_url, service))
            status_xml = self.request_status(service)

            self.set_status_for_service(status_xml)

        return self.status

    def set_status_for_service(self, service, status_xml):
        """
        Set status dict based on values from status_xml. Return status for the service.
        """
        if self.status[service] is None:
            self.status[service]['data_providers'] = ()

        self.status[service]['data_providers'] = WDB2TS.data_providers_from_status_response(status_xml)

        if len(self.status[service]['data_providers']) == 0:
            logging.warn("WDB2TS data providers for service %s set to empty list." % service)

        return self.status[service]

    @staticmethod
    def data_providers_from_status_response(status_xml):
        """
        Get all defined data_providers from status_xml.
        """
        tree = lxml.etree.fromstring(status_xml)
        provider_elements = tree.xpath('/status/defined_dataproviders/dataprovider/name')

        return [e.text for e in provider_elements]

    def __repr__(self):
        return "WDB2TS(%s, %s)" % (self.base_url, ",".join(self.status.keys()))


class WDB(object):

    def __init__(self, host, user):
        self.host = host
        self.user = user

    def load_model_run(self, model, model_run):
        """Load into wdb all relevant data from a model_run."""

        logging.info("Starting loading to WDB: %s" % model_run)

        data_uri_pattern = model.data_uri_pattern

        for data in model_run.data:
            data_uri = data.href

            if re.search(data_uri_pattern, data_uri) is not None:
                logging.info("Data URI '%s' matches regular expression '%s'" % (data_uri, data_uri_pattern))
                modelfile = WDB.convert_opdata_uri_to_file(data_uri)
                self.load_modelfile(model, modelfile)

        logging.info("Successfully finished loading to WDB." % model_run)

    def load_modelfile(self, model, modelfile):
        """Load a modelfile into wdb."""

        logging.info("Loading file %s" % modelfile)

        load_cmd = WDB.create_load_command(model, modelfile)
        cmd = self.create_ssh_command(load_cmd)

        logging.debug("Load command: %s" % cmd)

        try:
            exit_code, stderr, stdout = WDB.execute_command(cmd)
        except TypeError, e:
            raise syncer.exceptions.WDBLoadFailed("WDB load failed due to malformed command %s" % e)

        if stderr is not None:
            logging.warning("WDB load might have failed due to the following messages in stderr:")
            for line in stderr.splitlines():
                logging.warning("WDB load error: " + line)

        if exit_code > 0:
            raise syncer.exceptions.WDBLoadFailed("WDB load failed with exit code %d" % exit_code)

        logging.info("Loading completed.")

    @staticmethod
    def execute_command(cmd):
        """Executes a shell command.

        cmd: A command represented by a list of arguments.
        Returns three values: exit_code(int), stderr(string) and stdout(string).
        """
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = process.communicate()
        exit_code = process.returncode

        return exit_code, stderr, stdout

    @staticmethod
    def create_load_command(model, modelfile):
        """Generate a wdb load command for a specific model and modelfile, based on info from config."""

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

        # Most recent model run according to web service
        self.current_model_run = None
        self.current_model_run_initialized = False

        # Model run loaded into WDB
        self.wdb_model_run = None

    @staticmethod
    def data_from_config_section(config, section_name):
        """Return config options for a model. Raise exception if mandatory config option is missing"""

        data = {}
        mandatory_options = ['data_provider', 'data_uri_pattern', 'load_program', 'load_config']

        section_keys = config.section_keys(section_name)
        for option in mandatory_options:
            if option not in section_keys:
                raise ConfigParser.NoOptionError(option, section_name)

        data = config.section_options(section_name)

        return data

    def _validate_model_run(self, model_run):
        if model_run is not None and not isinstance(model_run, syncer.rest.BaseResource):
            raise TypeError("%s argument 'model_run' must inherit from syncer.rest.BaseResource" % sys._getframe().f_code.co_name)

    def set_current_model_run(self, model_run):
        self._validate_model_run(model_run)
        self.current_model_run = model_run
        self.current_model_run_initialized = True
        logging.info("Model %s has new model run: %s" % (self, self.current_model_run))

    def set_wdb_model_run(self, model_run):
        self._validate_model_run(model_run)
        self.wdb_model_run = model_run
        logging.info("Model %s has been loaded into WDB, model run: %s" % (self, self.current_model_run))

    def has_pending_model_run(self):
        if self.current_model_run_initialized:
            return self.wdb_model_run != self.current_model_run
        return False

    def __repr__(self):
        return self.data_provider


class Daemon:
    def __init__(self, config, models, zmq, wdb, wdb2ts, model_run_collection, data_collection):
        self.config = config
        self.models = models
        self.zmq = zmq
        self.wdb = wdb
        self.wdb2ts = wdb2ts
        self.model_run_collection = model_run_collection
        self.data_collection = data_collection

        if not isinstance(models, set):
            raise TypeError("'models' must be a set of models")
        for model in self.models:
            if not isinstance(model, Model):
                raise TypeError("'models' set must contain only models")

        logging.info("Daemon initialized with the following model configuration:")
        num_models = len(self.models)
        for num, model in enumerate(self.models):
            logging.info(" %2d of %2d: %s" % (num_models, num + 1, model.data_provider))

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

        # Server threw an error, recover from that
        except syncer.exceptions.RESTException, e:
            logging.error("REST API threw up with an exception: %s" % e)

    def handle_zmq_event(self, event):
        logging.info("Received %s" % unicode(event))
        if event.resource == 'model_run':
            id = event.id
        elif event.resource == 'data':
            data_object = self.data_collection.get_object(event.id)
            id = data_object.model_run_id
        else:
            logging.info("Nothing to do with this kind of event; no action taken.")
            return

        model_run_object = self.model_run_collection.get_object(id)

        for model in self.models:
            if model.data_provider == model_run_object.data_provider:
                model.set_current_model_run(model_run_object)
                return

        logging.info("No models configured to handle this event; no action taken.")

    def load_model(self, model):
        """
        Load the latest model run of a certain model into WDB
        """
        try:
            self.wdb.load_model_run(model, model.current_model_run)
            model.set_wdb_model_run(model.current_model_run)

        except syncer.exceptions.WDBLoadFailed, e:
            logging.error("WDB load failed: %s" % e)
        except syncer.exceptions.OpdataURIException, e:
            logging.error("Failed to load some model data due to erroneous opdata uri: %s" % e)

    def main_loop_inner(self):
        """Workhorse of the main loop"""

        # Check if we've got something from the Modelstatus ZeroMQ publisher
        zmq_event = self.zmq.get_event()
        if zmq_event:
            try:
                self.handle_zmq_event(zmq_event)
            except syncer.exceptions.RESTException, e:
                logging.error("Server returned invalid resource: %s" % e)

        # Try to initialize all un-initialized models with current model run status
        for model in self.models:
            if not model.current_model_run_initialized:
                logging.info("Model %s does not have any information about model runs, initializing from API..." % model)
                self.get_latest_model_run(model)

        # Loop through models and see which are not loaded yet
        for model in self.models:
            if model.has_pending_model_run():
                logging.info("Model %s has a pending model run not yet loaded into WDB." % model)
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
        wdb = WDB(config.get('wdb', 'host'), config.get('wdb', 'ssh_user'))

        # Get all wdb2ts services from comma separated list in config
        wdb2ts_services = [s.strip() for s in config.get('wdb2ts', 'services').split(',')]
        wdb2ts = WDB2TS(config.get('wdb2ts', 'base_url'), wdb2ts_services)
    except ConfigParser.NoOptionError, e:
        logging.critical("Missing configuration for WDB host")
        return EXIT_CONFIG

    # Start main application
    logging.info("Syncer is started")
    model_keys = set([model.strip() for model in config.get('syncer', 'models').split(',')])
    models = set([Model(Model.data_from_config_section(config, 'model_%s' % key)) for key in model_keys])
    base_url = config.get('webservice', 'url')
    verify_ssl = bool(int(config.get('webservice', 'verify_ssl')))

    zmq_socket = config.get('zeromq', 'socket')
    zmq = syncer.zeromq.ZMQSubscriber(zmq_socket)

    logging.info("ZeroMQ subscriber listening for events from %s" % zmq_socket)
    model_run_collection = syncer.rest.ModelRunCollection(base_url, verify_ssl)
    data_collection = syncer.rest.DataCollection(base_url, verify_ssl)

    daemon = Daemon(config, models, zmq, wdb, wdb2ts, model_run_collection, data_collection)
    exit_code = daemon.run()

    return exit_code


def main(argv):
    # Set up default initial logging, in case something goes wrong during config parsing
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=DEFAULT_LOG_LEVEL)
    logging.info("Starting Syncer...")

    exit_code = run(argv)

    logging.info("Exiting with status %d", exit_code)
    sys.exit(exit_code)
