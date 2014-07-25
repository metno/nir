import zmq
import django.db
import django.db.models.signals
import django.dispatch
import core.lib.zeromq

from django.db import models
from django.conf import settings


class Model(models.Model):
    id = models.CharField(primary_key=True, max_length=255, null=False, blank=False)
    name = models.CharField(max_length=255)

    def __unicode__(self):
        return "%s (%s)" % (self.name, self.id)


class Dataset(models.Model):
    STATUS_NEW = 0
    STATUS_COMPLETE = 1

    STATUS_CHOICES = (
            (STATUS_NEW, 'New'),
            (STATUS_COMPLETE, 'Complete'),
            )

    model = models.ForeignKey(Model, related_name='datasets')
    date = models.DateField()
    term = models.IntegerField()
    status = models.IntegerField(choices=STATUS_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    modified_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ('model', 'date', 'term')

    def __unicode__(self):
        return "%s @ %s term %.2d" % (self.model.id, unicode(self.date), self.term)


class File(models.Model):
    dataset = models.ForeignKey(Dataset, related_name='files')
    uri = models.CharField(max_length=512, null=False)

    class Meta:
        unique_together = ('dataset', 'uri')


#
# Push weather model status updates to ZMQ
#
def wms_zmq_pusher(sender, **kwargs):
    if kwargs['instance'].status == Dataset.STATUS_COMPLETE:
        django.db.connection.on_commit(lambda: core.lib.zeromq.send_ipc_message(kwargs['instance'].id))

django.db.models.signals.post_save.connect(wms_zmq_pusher, sender=Dataset, dispatch_uid='wms_zmq_pusher')
