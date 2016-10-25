import configparser
from datetime import datetime, timedelta
import dateutil
import logging
import time
import threading
import uuid
import productstatus.api
import kafka.errors
import syncer


class Listener(threading.Thread, syncer._util.SyncerBase):
    '''
    Listens on productstatus' kafka queue for events.
    '''

    def __init__(self, config):
        threading.Thread.__init__(self)
        syncer._util.SyncerBase.__init__(self, config)
        try:
            self.api = self.get_productstatus_api()
            self.models = self.get_model_setup()
            self.max_heartbeat_delay = int(config.get('productstatus', 'max_heartbeat_delay', 0))
            self.group_id = 'syncer_' + str(uuid.uuid4())
            self._reset_kafka_connection()
            self.new_data = threading.Event()
            self.stopping = False
        except configparser.Error as e:
            raise syncer.exceptions.ConfigurationException(e)

    def stop(self):
        self.stopping = True
        self.new_data.set()

    def _reset_kafka_connection(self):
        logging.info('Connecting to %s' % (self.api))
        while True:
            try:
                self.productstatus_listener = self.api.get_event_listener(consumer_timeout_ms=10000,
                                                                          group_id=self.group_id)
                break
            except kafka.errors.KafkaError as e:
                if e.retriable:
                    if str(e):
                        logging.warning(str(e))
                    else:  # kafka errors seem to often come without any explanatory text
                        logging.warning(type(e).__name__)
                    time.sleep(5)
                else:
                    raise

    def _threaded_init(self):
        '''Stuff that has to be run in the provided thread'''
        # sqlite connections may only be used from the thread that created them
        self._state_database = self.get_state_database_connection()
        self.reporter = self.get_reporter(self._state_database)

    def run(self):
        '''Run the main event loop'''
        self._threaded_init()

        logging.debug('Awaiting events')
        self._listen_for_new_events()
        self.new_data.set()  # May be used to detect that we have started listening.
        while not self.stopping:
            try:
                self._listen_for_new_events()
            except productstatus.exceptions.ServiceUnavailableException:
                logging.warn('productstatus service temporarily unavailable. Will retry later')
                time.sleep(1)
        logging.info('Exiting daemon loop')

    def _listen_for_new_events(self):
        while True:
            try:
                event = self.productstatus_listener.get_next_event()
                self._incoming_event(event)
                self.productstatus_listener.save_position()
                return event
            except productstatus.exceptions.EventTimeoutException:
                self._check_heartbeat_status()
                return None
            except kafka.errors.CommitFailedError as e:
                logging.warn('Error when contacting kafka: "%s". Resetting connection.' % e)
                self._reset_kafka_connection()
            except kafka.errors.KafkaError as e:
                if not e.retriable:
                    raise
                logging.warn('Error when contacting kafka: "%s". Retrying...' % e)

    def _incoming_event(self, event):
        '''Process an event coming from productstatus kafka queue'''
        try:
            event_type = event['type']
            if event_type == 'resource':
                self._process_resource_event(event)
            elif event_type == 'heartbeat':
                self._process_heartbeat_event(event)
        except KeyError:
            logging.warn('Did not understand event from kafka: ' + str(event))

    def _process_resource_event(self, event):
        if event['resource'] == 'datainstance':
            datainstance = self._get_datainstance(event)
            got_relevant_datainstance = False
            for model in self._get_models_for(datainstance):
                logging.debug('Relevant event for %s: %s' % (model, str(event)))
                got_relevant_datainstance = True
                productinstance = datainstance.data.productinstance
                self.reporter.report_data_event(model.model, syncer.persistence.StateDatabase.DATA_AVAILABLE, productinstance)
                self._state_database.add_productinstance_to_be_processed(productinstance)
            if got_relevant_datainstance:
                self.new_data.set()

    def _get_datainstance(self, event):
        # Ensure that all events are at least two seconds old
        timestamp = event['message_timestamp']
        creation_time = unserialize_datetime(timestamp)
        event_age = (datetime.now(tz=dateutil.tz.tzutc()) - creation_time).total_seconds()
        target_event_age = 2.5
        if event_age < target_event_age:
            sleep_time = target_event_age - event_age
            time.sleep(sleep_time)

        return self.api.datainstance[event['id']]

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

    def _process_heartbeat_event(self, event):
        """
        Register timestamp of last kafka heartbeat message received.
        Log warning if heartbeat is stale.
        """
        this_heartbeat = unserialize_datetime(event['message_timestamp'])
        if hasattr(self, 'last_heartbeat'):
            if (this_heartbeat - self.last_heartbeat) > timedelta(minutes=2):
                logging.warn('More than 2 minutes between productstatus internal heartbeats (%s - %s)' % (this_heartbeat, self.last_heartbeat))
        self.last_heartbeat = this_heartbeat

    def _check_heartbeat_status(self):
        """Exit if we have stopped receiving kafka heartbeat messages."""
        now = datetime.utcnow().replace(tzinfo=dateutil.tz.tzutc())
        if (hasattr(self, 'last_heartbeat') and self.max_heartbeat_delay > 0):
            if (now - self.last_heartbeat) > timedelta(minutes=self.max_heartbeat_delay):
                logging.error("No kafka heartbeat messages received for at least %s minutes. Recreating kafka connection." % self.max_heartbeat_delay)
                self._reset_kafka_connection()


def unserialize_datetime(timestamp):
    s = productstatus.utils.SerializeBase()
    return s._unserialize_datetime(timestamp)
