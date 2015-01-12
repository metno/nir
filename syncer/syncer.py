#!/usr/bin/env python2.7

import sys
import ConfigParser

def read_config_file(parser, config_file):
    parser.readfp(config_file)

def main():
    parser = ConfigParser.SafeConfigParser()
    read_config_file(parser, open('config.ini'))
    return 0

if __name__ == '__main__':
    sys.exit(main())
