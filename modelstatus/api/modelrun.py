#!/usr/bin/env python2.7

import falcon
import dateutil
import dateutil.parser

import modelstatus.utils
import modelstatus.api
import modelstatus.orm


class BaseResource(object):

    def normalize_reference_time(self, value):
        value = dateutil.parser.parse(value)
        value = value.astimezone(dateutil.tz.gettz('UTC'))
        return value


class CollectionResource(BaseResource, modelstatus.api.BaseCollectionResource):
    orm_class = modelstatus.orm.ModelRun

    @falcon.before(modelstatus.utils.deserialize)
    @falcon.after(modelstatus.utils.serialize)
    def on_post(self, req, resp, doc):
        """
        Create a new model_run resource.
        """

        # version is auto assigned and should not be part of the request
        try:
            if 'version' in doc:
                raise AttributeError("The 'version' parameter is not allowed on POST requests to a collection, it will be auto-assigned.")
        except AttributeError as e:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request', unicode(e))

        norm_doc = self.normalize_attributes(doc)
        query = self.orm.query(self.orm_class)
        query = query.filter(self.orm_class.reference_time == norm_doc['reference_time'],
                             self.orm_class.data_provider == norm_doc['data_provider']) \
                             .order_by(self.orm_class.version.desc()) \
                             .limit(1)
        latest = query.all()
        if latest:
            doc['version'] = latest[0].version + 1
        else:
            doc['version'] = 1

        self._on_post(req, resp, doc) # actually create the resource


class ItemResource(BaseResource, modelstatus.api.BaseItemResource):
    orm_class = modelstatus.orm.ModelRun
