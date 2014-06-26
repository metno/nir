from django.db import models

class WeatherModel(models.Model):
    id = models.CharField(primary_key=True, max_length=255)
    name = models.CharField(null=True, max_length=255)

class WeatherModelRun(models.Model):
    weathermodel = models.ForeignKey(WeatherModel)
    date = models.DateField()
    term = models.IntegerField()
    status = models.IntegerField()
    datetime = models.DateTimeField()

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
