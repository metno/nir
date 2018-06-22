import productstatus.api
import syncer.config
import syncer.reporting


class SyncerBase(object):

    def __init__(self, config):
        self.config = config

    def get_productstatus_api(self):
        base_url = self.config.get('productstatus', 'url')
        verify_ssl = bool(int(self.config.get('productstatus', 'verify_ssl')))
        return productstatus.api.Api(base_url, verify_ssl=verify_ssl)

    def get_state_database_connection(self):
        state_database_file = self.config.get('syncer', 'state_database_file')
        return syncer.persistence.StateDatabase(state_database_file, create_if_missing=True)

    def get_reporter(self, state_database):
        return syncer.reporting.StoringStatsClient(state_database)

def get_model_setup(config):
    model_keys = set([model.strip() for model in config.get('syncer', 'models').split(',')])
    models = set()
    for key in model_keys:
        models.add(syncer.config.ModelConfig.from_config_section(config, key))
    return models
