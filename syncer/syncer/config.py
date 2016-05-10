import argparse
import logging
import sys
import ConfigParser


DEFAULT_CONFIG_PATH = '/etc/syncer.ini'

EXIT_SUCCESS = 0
EXIT_CONFIG = 1
EXIT_LOGGING = 2
EXIT_CONNECT_PRODUCTSTATUS = 3


class ModelConfig(object):

    def __init__(self, data):
        [setattr(self, key, value) for key, value in data.iteritems()]

    @staticmethod
    def from_config_section(config, section_name):
        """Return config options for a model. Raise exception if mandatory config option is missing"""

        data = {}
        mandatory_options = ['data_uri_pattern', 'load_program']

        section_keys = config.section_keys(section_name)
        for option in mandatory_options:
            if option not in section_keys:
                raise ConfigParser.NoOptionError(option, section_name)

        data = config.section_options(section_name)

        for param in ['model_run_age_warning', 'model_run_age_critical']:
            if param in data:
                data[param] = int(data[param])

        return ModelConfig(data)


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


def get_config(argv):
    # Parse command line arguments and read the configuration file
    try:
        config = Configuration()
        config.parse_args(argv)
        config.load(open(config.args.config))
        return config
    except IOError, e:
        logging.critical("Could not read configuration file: %s" % unicode(e))
        sys.exit(EXIT_CONFIG)
    except Exception, e:
        logging.critical("Unhandled exception while loading configuration: %s" % unicode(e))
        raise e
