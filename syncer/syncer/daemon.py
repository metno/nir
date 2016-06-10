import dateutil
import logging
import time
import configparser
import productstatus.exceptions
import productstatus.api

import syncer.exceptions
import syncer.persistence
import syncer.reporting
from datetime import datetime


class Daemon(object):
    '''
    Listens for events on productstatus kafka queue, and handles them.
    '''
    def __init__(self, config):
        try:
            base_url = config.get('productstatus', 'url')
            verify_ssl = bool(int(config.get('productstatus', 'verify_ssl')))

            model_keys = set([model.strip() for model in config.get('syncer', 'models').split(',')])
            self.models = set()
            for key in model_keys:
                self.models.add(syncer.config.ModelConfig.from_config_section(config, key))

            wdb_host = config.get('wdb', 'host')
            wdb_user = config.get('wdb', 'user')
            self.wdb = syncer.wdb.WDB(wdb_host, wdb_user)

            state_database_file = config.get('syncer', 'state_database_file')
            self._state_database = syncer.persistence.StateDatabase(state_database_file, create_if_missing=True)

            self.reporter = syncer.reporting.StoringStatsClient(self._state_database)

            # Get all wdb2ts services from comma separated list in config
            wdb2ts_services = [s.strip() for s in config.get('wdb2ts', 'services').split(',')]
            self.wdb2ts = syncer.wdb2ts.WDB2TS(config.get('wdb2ts', 'base_url'), wdb2ts_services)
        except configparser.Error as e:
            raise syncer.exceptions.ConfigurationException(e)

        logging.info('Connecting to %s' % (base_url))

        self.api = productstatus.api.Api(base_url, verify_ssl=verify_ssl)
        self.productstatus_listener = self.api.get_event_listener(consumer_timeout_ms=10000)

    def run(self):
        '''Run the main event loop'''
        logging.debug('Awaiting events')
        while True:
            try:
                # Collect any old events
                self._add_latest_events_from_server()
                while True:
                    self._process_pending_productinstances()
                    self._listen_for_new_events()
            except productstatus.exceptions.ServiceUnavailableException:
                logging.warn('productstatus service temporarily unavailable. Will retry later')
                time.sleep(1)
            except KeyboardInterrupt:
                logging.debug('Keyboard interrupt')
                break
        logging.info('Exiting daemon loop')

    def _listen_for_new_events(self):
        try:
            event = self.productstatus_listener.get_next_event()
            self._incoming_event(event)
            self.productstatus_listener.save_position()
            return event
        except productstatus.exceptions.EventTimeoutException:
            return None

    def _add_latest_events_from_server(self):
        '''Find the latest events from server, and add them to the processing queue'''
        products = [m.product for m in self.models]
        for p in products:
            product = self.api.product[p]
            logging.info('Product: %s' % (product.slug))
            productinstances = self.api.productinstance.objects
            productinstances.filter(product=product)
            productinstances.order_by('-reference_time')
            productinstances.limit(2)  # two in case the latest is not yet complete
            count = productinstances.count()
            if not count:
                logging.info('Product <%s> has no instance yet' % (product.slug))
            for idx in range(min(2, count)):
                pi = productinstances[idx]
                self._state_database.add_productinstance_to_be_processed(pi)

    def _process_pending_productinstances(self):
        for productinstance_id, force in list(self._state_database.pending_productinstances().items()):
            productinstance = self.api.productinstance[productinstance_id]
            logging.debug('Pending productinstance %s. Force=%s' % (productinstance_id, force))
            if not self._state_database.is_loaded(productinstance_id):
                logging.debug('Processing')
                self._process_productinstance(productinstance, force)
            else:
                logging.debug('Already processed')
                self._state_database.done(productinstance)

    def _incoming_event(self, event):
        '''Process an event coming from productstatus kafka queue'''
        try:
            if event['resource'] == 'datainstance':
                datainstance = self._get_datainstance(event)
                for model in self._get_models_for(datainstance):
                    logging.debug('Relevant event for %s: %s' % (model, str(event)))
                    productinstance = datainstance.data.productinstance
                    self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_AVAILABLE, productinstance)
                    self._state_database.add_productinstance_to_be_processed(productinstance)
        except KeyError:
            logging.warn('Did not understand event from kafka: ' + str(event))

    def _get_models_for(self, datainstance):
        ret = []
        if datainstance:
            servicebackend = datainstance.servicebackend
            product = datainstance.data.productinstance.product
            for m in self.models:
                model_match = m.product in (product.slug, product.id)
                backend_match = m.servicebackend in (servicebackend.slug, servicebackend.id)
                if model_match and backend_match:
                    ret.append(m)
        return ret

    def _process_productinstance(self, productinstance, force):
        models = {}
        for datainstance in self.api.datainstance.objects.filter(data__productinstance=productinstance):
            for m in self.models:
                if m.servicebackend in (datainstance.servicebackend.slug, datainstance.servicebackend.id):
                    if m in models:
                        models[m].append(datainstance)
                    else:
                        models[m] = [datainstance]

        if force and not models:
            logging.error('Trying to force load productinstance %s, but unable to find model config!')
            return

        for model, datainstances in models.items():
            complete = productinstance.complete[datainstances[0].servicebackend.resource_uri][datainstances[0].format.resource_uri]
            if force or complete:
                try:
                    reporter = self.reporter.time_reporter()
                    self.reporter.incr('load start', 1)
                    for instance in datainstances:
                        di = DataInstance(instance, model)
                        self.wdb.load_model_file(di)
                    self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_WDB_OK, productinstance)
                    reporter.report('wdb load')
                    self.wdb.cache_model_run(di)
                    reporter.report('wdb cache')
                    self.wdb2ts.update(di)
                    self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_WDB2TS_OK, productinstance)
                    reporter.report('wdb2ts update')
                    self._state_database.set_loaded(productinstance.id)
                    self._state_database.done(productinstance)
                    reporter.report_total('productinstance time to complete')
                    self.reporter.incr('load end', 1)
                    self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_DONE, productinstance)
                except (syncer.exceptions.WDBLoadFailed, syncer.exceptions.WDBCacheFailed, syncer.exceptions.WDB2TSException) as e:
                    self.reporter.incr('load failed', 1)
                    logging.error('Error when loading data: ' + str(e))

    def _get_datainstance(self, event):
        # Ensure that all events are at least two seconds old
        timestamp = event['message_timestamp']
        s = productstatus.utils.SerializeBase()
        creation_time = s._unserialize_datetime(timestamp)
        event_age = (datetime.now(tz=dateutil.tz.tzutc()) - creation_time).total_seconds()
        target_event_age = 2.5
        if event_age < target_event_age:
            sleep_time = target_event_age - event_age
            time.sleep(sleep_time)

        return self.api.datainstance[event['id']]


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
        return self.model.data_provider
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
