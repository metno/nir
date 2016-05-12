import dateutil
import logging
import re
import time
import ConfigParser
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
            client_id = config.get('productstatus', 'client_id') if 'client_id' in config.section_keys('productstatus') else None
            group_id = config.get('productstatus', 'group_id') if 'group_id' in config.section_keys('productstatus') else None

            model_keys = set([model.strip() for model in config.get('syncer', 'models').split(',')])
            self.models = set()
            for key in model_keys:
                self.models.add(syncer.config.ModelConfig.from_config_section(config, 'model_%s' % key))

            wdb_host = config.get('wdb', 'host')
            wdb_ssh_user = config.get('wdb', 'ssh_user')
            self.wdb = syncer.wdb.WDB(wdb_host, wdb_ssh_user)

            state_database_file = config.get('syncer', 'state_database_file')
            self._state_database = syncer.persistence.StateDatabase(state_database_file, create_if_missing=True)

            # Get all wdb2ts services from comma separated list in config
            wdb2ts_services = [s.strip() for s in config.get('wdb2ts', 'services').split(',')]
            self.wdb2ts = syncer.wdb2ts.WDB2TS(config.get('wdb2ts', 'base_url'), wdb2ts_services)
        except ConfigParser.Error, e:
            raise syncer.exceptions.ConfigurationException(e)

        logging.info('Connecting to %s, using client_id=%s and group_id=%s' % (base_url, client_id, group_id))

        self.api = productstatus.api.Api(base_url, verify_ssl=verify_ssl)
        self.productstatus_listener = self.api.get_event_listener(client_id=client_id,
                                                                  group_id=group_id,
                                                                  consumer_timeout_ms=1000)

    def run(self):
        '''Run the main event loop'''
        logging.debug('Awaiting events')
        while True:
            try:
                # Collect any old events
                while self._listen_for_new_events():
                    pass
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
            logging.debug('Got event: ' + str(event))
            self._incoming_event(event)
            self.productstatus_listener.save_position()
            return event
        except productstatus.exceptions.EventTimeoutException:
            return None

    def _process_pending_productinstances(self):
        for productinstance_id, force in self._state_database.pending_productinstances().items():
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
                if self._has_model_for(datainstance):
                    reference_time = datainstance.data.productinstance.reference_time
                    syncer.reporting.stats.gauge('reference_time last seen', int(time.mktime(reference_time.timetuple())))
                    if datainstance:
                        self._state_database.add_productinstance_to_be_processed(datainstance.data.productinstance)
        except KeyError:
            logging.warn('Did not understand event from kafka: ' + str(event))
            
    def _has_model_for(self, datainstance):
        for m in self.models:
            if re.match(m.data_uri_pattern, datainstance.url):
                return True
        return False

    def _should_process_productinstance(self, productinstance):
        try:
            for di in self.api.datainstance.objects.filter(data__productinstance=productinstance):
                servicebackend = di.servicebackend
                dataformat = di.format
                complete = productinstance.complete[servicebackend.resource_uri][dataformat.resource_uri]
                if complete['file_count']:
                    return True
            logging.info('Not yet ready for processing')
            return False
        except AttributeError:
            logging.warning('Unable to find completeness information about data <%s>. Considering incomplete.' % (productinstance.resource_uri,))
            return False

    def _process_productinstance(self, productinstance, force):
        try:
            should_process = self._should_process_productinstance(productinstance)
            if force or should_process:
                if not should_process:
                    logging.info('Data is not really ready for loading, but force=True. Attempting to load anyway')
                reporter = syncer.reporting.TimeReporter()
                syncer.reporting.stats.incr('load start', 1)
                for instance in self.api.datainstance.objects.filter(data__productinstance=productinstance):
                    di = DataInstance(instance, self.models)
                    self.wdb.load_model_file(di)
                reporter.report('wdb load')
                self.wdb.cache_model_run(di)
                reporter.report('wdb cache')
                self.wdb2ts.update(di)
                reporter.report('wdb2ts update')
                self._state_database.set_loaded(productinstance.id)
                reporter.report_total('productinstance time to complete')
                syncer.reporting.stats.incr('load end', 1)
                syncer.reporting.stats.gauge('reference_time last successful', int(time.mktime(productinstance.reference_time.timetuple())))
        except (syncer.exceptions.WDBLoadFailed, syncer.exceptions.WDBCacheFailed, syncer.exceptions.WDB2TSException) as e:
            syncer.reporting.stats.incr('load failed', 1)
            logging.error('Error when loading data: ' + unicode(e))

    def _get_datainstance(self, event):
        # Ensure that all events are at least two seconds old
        timestamp = event['message_timestamp']
        s = productstatus.utils.SerializeBase()
        creation_time = s._unserialize_datetime(timestamp)
        event_age = (datetime.now(tz=dateutil.tz.tzutc()) - creation_time).total_seconds()
        target_event_age = 2.5
        if event_age < target_event_age:
            sleep_time = target_event_age - event_age
            logging.debug('Sleeping for %f seconds, to allow productstatus to stabilize' % (sleep_time,))
            time.sleep(sleep_time)

        return self.api.datainstance[event['id']]


class DataInstance(object):
    '''Easier access to datainstance, also makes usage in tests easier'''

    def __init__(self, referenced_datainstance, models):
        self._datainstance = referenced_datainstance
        self._productinstance = self._datainstance.data.productinstance
        self.model = None
        for m in models:
            if re.match(m.data_uri_pattern, self.url()):
                self.model = m

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
        return self._verify(self._productinstance.product.wdb_data_provider, 'data provider')

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
            i = DataInstance(instance, models)
            i.model = self.model
            yield i
