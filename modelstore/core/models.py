import zmq
import django.db
import django.db.models.signals
import django.dispatch

from django.db import models

class WeatherModel(models.Model):
    id = models.CharField(primary_key=True, max_length=255)
    name = models.CharField(null=True, max_length=255)

    def __unicode__(self):
        return "%s (%s)" % (self.name, self.id)

class WeatherModelRun(models.Model):
    weathermodel = models.ForeignKey(WeatherModel)
    date = models.DateField()
    term = models.IntegerField()
    status = models.IntegerField() # FIXME: remove
    datetime = models.DateTimeField()

    def __unicode__(self):
        return "%s @ %s term %.2d" % (self.weathermodel.id, unicode(self.date), self.term)

class WeatherModelStatus(models.Model):
    weathermodelrun = models.ForeignKey(WeatherModelRun)

    STATUS_INIT = 0
    STATUS_RUNNING = 1
    STATUS_COMPLETE = 2
    STATUS_FAILED = 3

    STATUS_CHOICES = (
            (STATUS_INIT, 'New'),
            (STATUS_RUNNING, 'Running'),
            (STATUS_COMPLETE, 'Complete'),
            (STATUS_FAILED, 'Failed'),
            )

    status = models.IntegerField(choices=STATUS_CHOICES)
    datetime = models.DateTimeField(auto_now=True)


#
# Push weather model status updates to ZMQ
#
def wms_zmq_pusher(sender, **kwargs):
    def worker():
        instance = kwargs['instance']
        context = zmq.Context(1)
        print instance.get_status_display()

        sock = context.socket(zmq.REQ)
        sock.connect('ipc:///tmp/zmq')

        poll = zmq.Poller()
        poll.register(sock, zmq.POLLIN)

        sock.send_string(unicode(instance.id))
        socks = dict(poll.poll(1000))
        if socks.get(sock) == zmq.POLLIN:
            reply = sock.recv_string()
            print reply
            assert reply == 'OK'

        sock.close()
        context.term()

    django.db.connection.on_commit(worker)

django.db.models.signals.post_save.connect(wms_zmq_pusher, sender=WeatherModelStatus, dispatch_uid='wms_zmq_pusher')
