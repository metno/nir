#!/usr/bin/env python2.7

import falcon
import json

import modelstatus.utils
import modelstatus.api
import modelstatus.orm


class CollectionResource(modelstatus.api.BaseCollectionResource):
    orm_class = modelstatus.orm.Data


class ItemResource(modelstatus.api.BaseItemResource):
    orm_class = modelstatus.orm.Data
