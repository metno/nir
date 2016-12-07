import configparser
import logging
import time
import syncer._util
import syncer.exceptions
import syncer.persistence


class DataLoader(syncer._util.SyncerBase):
    '''Loads data into wdb and wdb2ts'''

    def __init__(self, config, sleep_time_on_error=10):
        syncer._util.SyncerBase.__init__(self, config)
        self.sleep_time_on_error = sleep_time_on_error
        try:
            self.api = self.get_productstatus_api()
            self.models = self.get_model_setup()
            self._state_database = self.get_state_database_connection()
            self.reporter = self.get_reporter(self._state_database)

            wdb_host = config.get('wdb', 'host')
            wdb_user = config.get('wdb', 'user')
            self.wdb = syncer.wdb.WDB(wdb_host, wdb_user)

            # Get all wdb2ts services from comma separated list in config
            wdb2ts_services = [s.strip() for s in config.get('wdb2ts', 'services').split(',')]
            self.wdb2ts = syncer.wdb2ts.WDB2TS(config.get('wdb2ts', 'base_url'), wdb2ts_services)
        except configparser.Error as e:
            raise syncer.exceptions.ConfigurationException(e)

    def process(self):
        pending = self._state_database.pending_productinstances()
        for productinstance_id, force in list(pending.items()):
            productinstance = self.api.productinstance[productinstance_id]
            logging.debug('Pending: %s. Force=%s' % (productinstance.resource_uri, force))
            if not self._state_database.is_loaded(productinstance_id):
                logging.debug('Processing')
                self._process_productinstance(productinstance, force)
            else:
                logging.debug('Already processed')
                self._state_database.done(productinstance)

    def populate_database_with_latest_events_from_server(self):
        '''Find the latest events from server, and add them to the processing queue.'''
        # Note that a product instance may not have associated datainstances when it is discovered here.
        for m in self.models:
            product = self.api.product[m.product]
            productinstances = self.api.productinstance.objects
            productinstances.filter(product=product)
            productinstances.order_by('-reference_time')
            productinstances.limit(1)
            count = productinstances.count()
            if count:
                pi = productinstances[0]
                self._state_database.add_productinstance_to_be_processed(pi)
            else:
                logging.info('Product <%s> has no instance yet' % (product.slug))

    def _process_productinstance(self, productinstance, force):
        models = self._get_datainstances(productinstance)

        if force and not models:
            logging.error('Trying to force load productinstance %s, but unable to find model config!')
        elif not models:
            logging.info('No data available for %s (yet)' % (productinstance.resource_uri))

        for model, backends in models.items():
            for datainstances in backends:
                # complete is buggy - see above
                complete = productinstance.complete[datainstances[0].servicebackend.resource_uri][datainstances[0].format.resource_uri]
                if force or complete:
                    try:
                        self._load_data(model, productinstance, datainstances)
                        return
                    except syncer.exceptions.WDBLoadFailed as e:
                        self._handle_loading_error(e)
                        # if loading fails, we put the model's foremost backend to the back of the list and retries
                        if len(model.servicebackend) > 1:
                            logging.info('Problems when loading %s data from servicebackend %s. Switching to servicebackend %s' % (model.product, model.servicebackend[0], model.servicebackend[1]))
                            model.servicebackend = model.servicebackend[1:] + [model.servicebackend[0]]
                    except (syncer.exceptions.WDBCacheFailed, syncer.exceptions.WDB2TSException) as e:
                        self._handle_loading_error(e)
                elif not complete:
                    logging.debug('Not complete')

    def _handle_loading_error(self, error):
        self.reporter.incr('load failed', 1)
        logging.error('Error when loading data: ' + str(error))
        if self.sleep_time_on_error:
            time.sleep(self.sleep_time_on_error)

    def _load_data(self, model, productinstance, datainstances):
        reporter = self.reporter.time_reporter()
        self.reporter.incr('load start', 1)
        for instance in datainstances:
            self.wdb.load_model_file(instance, model)
        self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_WDB_OK, productinstance)
        reporter.report('wdb load')
        self.wdb.cache_model_run(productinstance, model)
        reporter.report('wdb cache')
        self.wdb2ts.update(productinstance, model)
        self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_WDB2TS_OK, productinstance)
        reporter.report('wdb2ts update')
        self._state_database.set_loaded(productinstance.id)
        self._state_database.done(productinstance)
        reporter.report_total('productinstance time to complete')
        self.reporter.incr('load end', 1)
        self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_DONE, productinstance)

    def _get_datainstances(self, productinstance):
        ret = {}
        all_instances = self._get_datainstances_belonging_to(productinstance)
        for m in self.models:
            load_alternatives = []
            for backend in m.servicebackend:
                relevant_instances = [i for i in all_instances if backend in (i.servicebackend.slug, i.servicebackend.id)]
                if relevant_instances:
                    load_alternatives.append(relevant_instances)
            if load_alternatives:
                ret[m] = load_alternatives
        return ret

    def _get_datainstances_belonging_to(self, productinstance):
        return self.api.datainstance.objects.filter(data__productinstance=productinstance)
