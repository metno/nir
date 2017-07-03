# coding: utf-8

import sys
import syncer

if __name__ == '__main__':
    exit_code = syncer.main(sys.argv[1:])
    sys.exit(exit_code)
