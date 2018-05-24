# coding: utf-8

import logging
import sys
import syncer

if __name__ == '__main__':
    exit_code = syncer.main(sys.argv[1:])
    logging.info("Exiting with status code %d", exit_code)
    sys.exit(exit_code)
