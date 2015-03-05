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

    def load_model_run(self, model, model_run):
        """Load into wdb all relevant data from a model_run."""

        loaded = 0
        logging.info("Starting loading to WDB: %s" % model_run)

        dataset = model.get_matching_data(model_run.data)

        for data in dataset:
            logging.info("Data URI '%s' matches regular expression '%s'" % (data.href, model.data_uri_pattern))
            modelfile = WDB.convert_opdata_uri_to_file(data.href)
            self.load_modelfile(model, model_run, modelfile)
            loaded += 1

        if loaded:
            logging.info("Successfully finished loading %d files to WDB." % loaded)
        else:
            logging.warn("No files were loaded into WDB.")

    def load_modelfile(self, model, model_run, modelfile):
        """Load a modelfile into wdb."""

        logging.info("Loading file %s" % modelfile)

        load_cmd = WDB.create_load_command(model, model_run, modelfile)
        cmd = self.create_ssh_command(load_cmd)

        try:
            exit_code, stderr, stdout = WDB.execute_command(cmd)
        except TypeError, e:
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
                        logging.warning("WDB load error: " + line)

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
    def create_load_command(model, model_run, model_file):
        """Generate a wdb load command for a specific model configuration and model run, based on info from config."""

        cmd = [model.load_program, '--dataprovider', "'%s'" % model.data_provider]

        if hasattr(model, 'load_config'):
            cmd.extend(['-c', model.load_config])

        if hasattr(model, 'place_name'):
            cmd.extend(["--placename", model.place_name])
        else:
            cmd.extend(["--loadPlaceDefinition"])

        cmd.extend(["--dataversion", unicode(model_run.version)])

        cmd.append(model_file)

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

    @staticmethod
    def create_cache_query(model_run):
        """
        Generate a SQL/WCI query that caches a specific model run.
        """
        # SQL injection attacks would have to be configured in the
        # configuration file, as data_provider to the model in question.
        # Modelstatus would likely not contain information about such a model,
        # making it extremely unlikely that any malicious code could run here.
        return "SELECT wci.begin('wdb'); SELECT wci.cacheQuery(array['%(data_provider)s'], NULL, 'exact %(reference_time)s', NULL, NULL, NULL, array[-1])" % {
            'data_provider': model_run.data_provider,
            'reference_time': model_run.serialize_reference_time(model_run.reference_time),
        }

    @staticmethod
    def create_analyze_query():
        return 'ANALYZE'

    def create_cache_model_run_command(self, model_run):
        """
        Create an SSH command that runs cacheQuery and ANALYZE against the WDB
        server for the specified model run.
        """
        cache_query = WDB.create_cache_query(model_run)
        analyze_query = WDB.create_analyze_query()
        return self.create_ssh_command(['psql', '-c', "\"%s; %s;\"" % (cache_query, analyze_query)])

    def cache_model_run(self, model_run):
        """
        Run cacheQuery and ANALYZE against the WDB server for the specified model run.
        """
        logging.info("Updating WDB cache for %s" % model_run)

        cmd = self.create_cache_model_run_command(model_run)
        exit_code, stderr, stdout = WDB.execute_command(cmd)

        if exit_code == 0:
            logging.info("Cache updated successfully.")
            return

        logging.error("Cache update failed with exit status %d" % exit_code)

        for output in stdout, stderr:
            lines = self.get_std_lines(stdout)
            if lines:
                [logging.debug(line) for line in lines]

        raise syncer.exceptions.WDBCacheFailed("Cache update failed with exit status %d" % exit_code)
