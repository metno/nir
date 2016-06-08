import statsd
import time
import datetime

class StoringStatsClient(statsd.StatsClient):
    '''Should handle all needs for reporting from syncer application. Can be 
    used as a regular statsd client, or by using the extra functions'''
    def __init__(self, state_database):
        statsd.StatsClient.__init__(self, 'localhost', 8125)
        self.state_database = state_database
    
    def report_data_event(self, model, type, datainstanceid, reference_time):
        self.state_database.set_last_incoming(model, type, datainstanceid, reference_time)
        self.gauge(type + ' ' + model, 
                   (reference_time - datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)).total_seconds())
    
    def time_reporter(self):
        return TimeReporter(self)


class TimeReporter(object):
    '''Reporting times spent at stuff to the given StatsClient'''

    def __init__(self, stats_client):
        self.reporter = stats_client
        self.first_time = time.time()
        self.last = self.first_time

    def report(self, name):
        now = time.time()
        self.reporter.timing(name, int((now - self.last)*1000))
        self.last = now

    def report_total(self, name):
        now = time.time()
        self.reporter.timing(name, int((now - self.first_time)*1000))
