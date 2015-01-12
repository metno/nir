#!/usr/bin/env python2.7

import unittest
import ConfigParser
import StringIO
import syncer

config_file_contents = """
[wdb]
host=127.0.0.1
"""

class SyncerTest(unittest.TestCase):
    def test_read_config_file(self):
        config_file = StringIO.StringIO(config_file_contents)
        parser = ConfigParser.SafeConfigParser()
        syncer.read_config_file(parser, config_file)
        self.assertEqual(parser.get('wdb', 'host'), '127.0.0.1')

if __name__ == '__main__':
    unittest.main()
