from django.contrib import admin

import core.models

admin.site.register(core.models.WeatherModel)
admin.site.register(core.models.WeatherModelRun)
admin.site.register(core.models.WeatherModelStatus)
