import statsd
import time


stats = statsd.StatsClient('localhost', 8125)


class TimeReporter(object):
    '''Reporting times spent at stuff to statsd'''

    def __init__(self):
        self.first_time = time.time()
        self.last = self.first_time

    def report(self, name):
        now = time.time()
        stats.timing(name, int((now - self.last)*1000))
        self.last = now

    def report_total(self, name):
        now = time.time()
        stats.timing(name, int((now - self.first_time)*1000))
