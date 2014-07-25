from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

import tastypie.api
import core.api.resources

v1_api = tastypie.api.Api(api_name='v1')
v1_api.register(core.api.resources.ModelResource())
v1_api.register(core.api.resources.DatasetResource())
v1_api.register(core.api.resources.FileResource())

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'modelstore.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^admin/', include(admin.site.urls)),
    url(r'^api/', include(v1_api.urls)),
)
