import argparse
import logging
import sys
import configparser
import threading

DEFAULT_CONFIG_PATH = '/etc/syncer.ini'

EXIT_SUCCESS = 0
EXIT_CONFIG = 1
EXIT_LOGGING = 2
EXIT_CONNECT_PRODUCTSTATUS = 3


class ModelConfig(object):

    def __init__(self, model, product, servicebackend, data_provider, load_program, load_config, model_run_age_warning, model_run_age_critical):
        self._model = model
        self._product = product
        self._servicebackend = servicebackend
        self._data_provider = data_provider
        self._load_program = load_program
        self._load_config = load_config
        self._model_run_age_warning = model_run_age_warning
        self._model_run_age_critical = model_run_age_critical
        self._lock = threading.Lock()

    @staticmethod
    def from_config_section(config, model_name):
        """Return config options for a model. Raise exception if mandatory config option is missing"""

        section_name = 'model_%s' % (model_name,)

        data = {}
        mandatory_options = ['product', 'servicebackend', 'data_provider', 'load_program', 'model_run_age_warning']
        section_keys = config.section_keys(section_name)
        for option in mandatory_options:
            if option not in section_keys:
                raise configparser.NoOptionError(option, section_name)

        data = config.section_options(section_name)

        product = data['product']
        servicebackend = data['servicebackend']
        data_provider = data['data_provider']
        load_program = data['load_program']
        if 'load_config' in data:
            load_config = data['load_config']
        else:
            load_config = None
        model_run_age_warning = int(data['model_run_age_warning'])
        if 'model_run_age_critical' in data:
            model_run_age_critical = data['model_run_age_critical']
        else:
            model_run_age_critical = 0


        for param in ['model_run_age_warning', 'model_run_age_critical']:
            if param in data:
                data[param] = int(data[param])

        data['model'] = model_name

        return ModelConfig(model_name, product, servicebackend, data_provider, load_program, load_config, model_run_age_warning, model_run_age_critical)

    def model(self):
        return self._model

    def product(self): 
        return self._product

    def servicebackend(self): 
        self._lock.acquire()
        ret = self._servicebackend.split(',', 1)[0]
        self._lock.release()
        return ret

    def servicebackends(self):
        self._lock.acquire()
        ret = self._servicebackend.split(',')
        self._lock.release()
        return ret

    def rotate_servicebackend(self):
        self._lock.acquire()
        backends = self._servicebackend.split(',', 1)
        if len(backends) > 1:
            logging.info('Switching to service backend ' + backends[1].split(',')[0])
            self._servicebackend = backends[1] + ',' + backends[0]
        self._lock.release()

    def data_provider(self): 
        return self._data_provider

    def load_program(self): 
        return self._load_program

    def load_config(self): 
        return self._load_config
    
    def model_run_age_warning(self):
        return self._model_run_age_warning
    
    def model_run_age_critical(self):
        return self._model_run_age_critical


class Configuration(object):
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
        return configparser.SafeConfigParser()

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

    def get(self, section, key, default=None):
        """
        Return config value if it exists. If no config key is found,
        return specified default or raise exception.
        """
        if default is not None:
            try:
                return self.config_parser.get(section, key)
            except configparser.NoOptionError:
                return default

        return self.config_parser.get(section, key)

    def section_keys(self, section_name):
        return [x[0] for x in self.config_parser.items(section_name)]

    def section_options(self, section_name):
        return dict(self.config_parser.items(section_name))


def get_config(argv):
    # Parse command line arguments and read the configuration file
    try:
        config = Configuration()
        config.parse_args(argv)
        config.load(open(config.args.config))
        return config
    except IOError as e:
        logging.critical("Could not read configuration file: %s" % str(e))
        sys.exit(EXIT_CONFIG)
    except Exception as e:
        logging.critical("Unhandled exception while loading configuration: %s" % str(e))
        raise e
