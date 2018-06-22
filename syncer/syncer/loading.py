import configparser
import logging
import time
import syncer._util
import syncer.exceptions
import syncer.persistence


class DataLoader(syncer._util.SyncerBase):
    '''Loads data into wdb and wdb2ts'''

    def __init__(self, config, models):
        syncer._util.SyncerBase.__init__(self, config)
        try:
            self.api = self.get_productstatus_api()
            self.models = models
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
            product = self.api.product[m.product()]
            productinstances = self.api.productinstance.objects
            productinstances.filter(product=product)
            productinstances.order_by('-reference_time')
            productinstances.limit(2)  # two in case the latest is not yet complete
            count = productinstances.count()
            if not count:
                logging.info('Product <%s> has no instance yet' % (product.slug))
            for idx in range(min(2, count)):
                pi = productinstances[idx]
                try:
                    servicebackend =  m.servicebackend()
                    # BUG: complete is buggy - it should be something like pi.complete[self.api.servicebackend[m.servicebackend].resource_uri][self.api.dataformat['netcdf'].resource_uri]['file_count']
                    # Will not fix yet until we are sure that also productstatus data is ok
                    complete = pi.complete[self.api.servicebackend[servicebackend].resource_uri][self.api.dataformat['netcdf'].resource_uri]
                except KeyError:
                    complete = False  # no completeness information available means not complete
                if complete:
                    self._state_database.add_productinstance_to_be_processed(pi)
                else:
                    logging.debug('Skipping %s on startup, since it seems to have no useable datainstance yet' % (pi.resource_uri,))

    def _process_productinstance(self, productinstance, force):
        models = {}
        for datainstance in self.api.datainstance.objects.filter(data__productinstance=productinstance):
            for m in self.models:
                servicebackend =  m.servicebackend()
                if m.product() in (productinstance.product.slug, productinstance.product.id):
                    if servicebackend in (datainstance.servicebackend.slug, datainstance.servicebackend.id):
                        if m in models:
                            models[m].append(datainstance)
                        else:
                            models[m] = [datainstance]

        if force and not models:
            logging.error('Trying to force load productinstance %s, but unable to find model config!')
        elif not models:
            logging.info('No data available for %s (yet)' % (productinstance.resource_uri))
        for model, datainstances in models.items():
            # complete is buggy - see above
            complete = productinstance.complete[datainstances[0].servicebackend.resource_uri][datainstances[0].format.resource_uri]
            if force or complete:
                try:
                    reporter = self.reporter.time_reporter()
                    self.reporter.incr('load start', 1)
                    for instance in datainstances:
                        di = DataInstance(instance, model)
                        self.wdb.load_model_file(di)
                    self.reporter.report_data_event(model.model(), syncer.persistence.StateDatabase.DATA_WDB_OK, productinstance)
                    reporter.report('wdb load')
                    self.wdb.cache_model_run(di)
                    reporter.report('wdb cache')
                    self.wdb2ts.update(di)
                    self.reporter.report_data_event(model.model(), syncer.persistence.StateDatabase.DATA_WDB2TS_OK, productinstance)
                    reporter.report('wdb2ts update')
                    self._state_database.set_loaded(productinstance.id)
                    self._state_database.done(productinstance)
                    reporter.report_total('productinstance time to complete')
                    self.reporter.incr('load end', 1)
                    self.reporter.report_data_event(model.model(), syncer.persistence.StateDatabase.DATA_DONE, productinstance)
                except (syncer.exceptions.WDBLoadFailed, syncer.exceptions.WDBCacheFailed, syncer.exceptions.WDB2TSException) as e:
                    self.reporter.incr('load failed', 1)
                    logging.error('Error when loading data: ' + str(e))
                    model.rotate_servicebackend()
                    time.sleep(10)
            elif not complete:
                logging.debug('Not complete')


class DataInstance(object):
    '''Easier access to datainstance, also makes usage in tests easier'''

    def __init__(self, referenced_datainstance, model):
        self._datainstance = referenced_datainstance
        self._productinstance = self._datainstance.data.productinstance
        self.model = model

    def _verify(self, to_return, name):
        if not to_return:
            raise syncer.exceptions.InvalidResourceException(name + ' is not available from server')
        return to_return

    def id(self):
        return self._verify(self._datainstance.id, 'id')

    def productinstance_id(self):
        return self._productinstance.id

    def url(self):
        return self._verify(self._datainstance.url, 'url')

    def data_provider(self):
        return self.model.data_provider()
#        return self._verify(self._productinstance.product.wdb_data_provider, 'data provider')

    def reference_time(self):
        return self._verify(self._productinstance.reference_time, 'reference time')

    def version(self):
        return self._verify(self._productinstance.version, 'data version')

    def complete_model_file_count(self):
        '''Get the number of files that together makes this model run complete'''
        return self._verify(self._productinstance.product.file_count, 'file_count')

    def is_complete(self):
        try:
            servicebackend = self._datainstance.servicebackend.resource_uri
            dataformat = self._datainstance.format.resource_uri
            complete = self._productinstance.complete[servicebackend][dataformat]
            return complete['file_count']
        except AttributeError:
            logging.warning('Unable to find completeness information about data <%s>. Considering incomplete.' % (self._productinstance.resource_uri,))
            return False

    def related(self, api, models):
        '''Get all datainstance objects with the same productinstance as this object'''
        for instance in api.datainstance.objects.filter(data__productinstance=self._productinstance):
            i = DataInstance(instance, self.model)
            yield i
