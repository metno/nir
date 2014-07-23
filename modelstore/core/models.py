import zmq
import django.db
import django.db.models.signals
import django.dispatch
import core.lib.zeromq

from django.db import models
from django.conf import settings

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
    django.db.connection.on_commit(lambda: core.lib.zeromq.send_ipc_message(kwargs['instance'].id))

django.db.models.signals.post_save.connect(wms_zmq_pusher, sender=WeatherModelStatus, dispatch_uid='wms_zmq_pusher')
