#!/usr/bin/env python2.7

import falcon
import json
import datetime
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


class ItemResource(BaseResource, modelstatus.api.BaseItemResource):
    orm_class = modelstatus.orm.ModelRun
