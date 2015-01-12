#!/usr/bin/env python2.7

import sys
import argparse
import ConfigParser

DEFAULT_CONFIG_PATH = '/etc/syncer.ini'

def read_config_file(parser, config_file):
    parser.readfp(config_file)

def create_argument_parser():
    return argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

def setup_argument_parser(parser):
    parser.add_argument('-c', '--config', help='path to configuration file', default=DEFAULT_CONFIG_PATH)

def parse_args(parser, args):
    return parser.parse_args(args)

def main():
    config_parser = ConfigParser.SafeConfigParser()
    argument_parser = create_argument_parser()
    setup_argument_parser(argument_parser)
    args = parse_args(argument_parser, sys.argv[1:])
    read_config_file(config_parser, open(args.config))
    return 0

if __name__ == '__main__':
    sys.exit(main())
