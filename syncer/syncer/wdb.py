"""
Functionality relating to WDB2.
"""

import re
import subprocess
import logging

import syncer.exceptions


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
