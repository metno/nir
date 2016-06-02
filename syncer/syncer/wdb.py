"""
Functionality relating to WDB2.
"""

import re
import subprocess
import logging

import syncer.exceptions

# Success
EXIT_SUCCESS = 0

# Unknown error - something no one anticipated
EXIT_UNKNOWN = 1

# Unable to connect to database
EXIT_DATABASE_CONNECT = 10

# Error when attempting to read from database
EXIT_DATABASE_READ = 11

# Error when updating database metadata
EXIT_DATABASE_METADATA = 12

# Unable to load any fields at all
EXIT_LOAD = 13

# Unable to read file
EXIT_FILE_OPEN = 20

# Error when reading from file
EXIT_FILE_READ = 21

# Invalid command line arguments
EXIT_SYNTAX = 30

# Error when attempting to read a configuration file
EXIT_CONFIG = 31

# One or more fields failed to load, but some may have loaded successfully
EXIT_FIELDS = 100


class WDB(object):

    def __init__(self, host, user):
        self.host = host
        self.user = user

    def load_model_file(self, datainstance):
        """Load a modelfile into wdb."""

        logging.info("Loading file %s" % datainstance.url())

        load_cmd = self.create_load_command(datainstance)
        cmd = self.create_ssh_command(load_cmd)

        try:
            exit_code, stderr, stdout = WDB.execute_command(cmd)
        except TypeError as e:
            raise syncer.exceptions.WDBLoadFailed("WDB load failed due to malformed command %s" % e)

        if exit_code != EXIT_SUCCESS:
            if exit_code == EXIT_FIELDS or exit_code == EXIT_LOAD:
                logging.error("Failed to load some fields into WDB. This is likely due to duplicate field errors, i.e. loading the same data twice.")
                logging.warn("STDERR output from WDB suppressed because exit code equals %d" % exit_code)
            else:
                lines = self.get_std_lines(stderr)
                if lines:
                    logging.warning("WDB load failed with exit code %d, STDERR output follows" % exit_code)
                    for line in lines:
                        logging.warning("WDB load error: " + str(line))

                raise syncer.exceptions.WDBLoadFailed("WDB load failed with exit code %d" % exit_code)

        logging.info("Loading completed.")

    def get_std_lines(self, std):
        """
        Return a list of lines from stderr or stdout
        """
        return std.splitlines() if std is not None else []

    @staticmethod
    def execute_command(cmd):
        """Executes a shell command.

        cmd: A command represented by a list of arguments.
        Returns three values: exit_code(int), stderr(string) and stdout(string).
        """
        logging.debug("Executing: %s" % ' '.join(cmd))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = process.communicate()
        exit_code = process.returncode

        return exit_code, stderr, stdout

    @staticmethod
    def clean_url(url):
        # TODO: Handle opdata url
        if url.startswith('file://'):
            return url[len('file://'):]
        elif re.match('opdata\:\/{3}(?!\/)', url):
            return re.sub(r'^opdata\:\/\/\/', '/opdata/', url)
        else:
            return url  # ...and hope for the best!

    @staticmethod
    def create_load_command(datainstance):
        load_command = [datainstance.model.load_program,
                        '--loadPlaceDefinition',
                        '--dataprovider', datainstance.data_provider()]
        
        if datainstance.model.ssh_user:
            load_command.append('--user')
            load_command.append(datainstance.model.ssh_user)

        if datainstance.version():
            load_command.append('--dataversion')
            load_command.append(str(datainstance.version()))

        if hasattr(datainstance.model, 'load_config'):
            load_command.append('--configuration')
            load_command.append(datainstance.model.load_config)

        load_command.append(WDB.clean_url(datainstance.url()))
        return load_command

    def should_use_ssh(self):
        return self.host not in ('localhost', '127.0.0.1')

    def create_ssh_command(self, cmd):
        if self.should_use_ssh():
            return ["ssh", "{0}@{1}".format(self.user, self.host)] + cmd
        else:
            return cmd

    @staticmethod
    def create_cache_query(datainstance):
        """
        Generate a SQL/WCI query that caches a specific model run.
        """
        # SQL injection attacks would have to be configured in the
        # configuration file, as data_provider to the model in question.
        # Modelstatus would likely not contain information about such a model,
        # making it extremely unlikely that any malicious code could run here.
        return "SELECT wci.begin('wdb'); SELECT wci.cacheQuery(array['%(data_provider)s'], NULL, 'exact %(reference_time)s', NULL, NULL, NULL, array[-1])" % {
            'data_provider': datainstance.data_provider(),
            'reference_time': datainstance.reference_time()
        }

    @staticmethod
    def create_analyze_query():
        return 'ANALYZE'

    def create_cache_model_run_command(self, datainstance):
        """
        Create an SSH command that runs cacheQuery and ANALYZE against the WDB
        server for the specified model run.
        """
        cache_query = WDB.create_cache_query(datainstance)
        analyze_query = WDB.create_analyze_query()

        sql_statement = cache_query + '; ' + analyze_query
        if self.should_use_ssh():
            # If you run this without ssh, this will fail, since the quotes surrounding the sql is not valid for subprocess.popen
            sql_statement = '"%s"' % (sql_statement,)
        return self.create_ssh_command(['psql', 'wdb', '-U', 'wdb', '-c', sql_statement])

    def cache_model_run(self, datainstance):
        """
        Run cacheQuery and ANALYZE against the WDB server for the specified model run.
        """
        logging.info("Updating WDB cache for %s" % datainstance.data_provider())

        cmd = self.create_cache_model_run_command(datainstance)
        error_code, stderr, stdout = WDB.execute_command(cmd)

        if error_code:
            logging.error("Cache update failed with exit status %d" % error_code)

            for output in stdout, stderr:
                lines = self.get_std_lines(stdout)
                if lines:
                    [logging.debug(str(line)) for line in lines]

            raise syncer.exceptions.WDBCacheFailed("Cache update failed with exit status %d" % error_code)
        else:
            logging.info("Cache updated successfully.")
