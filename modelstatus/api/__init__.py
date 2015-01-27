import falcon
import falcon.util.uri
import sqlalchemy.exc
import sqlalchemy.orm.exc

import modelstatus.utils
import modelstatus.zeromq

class BaseResource(object):
    """
    Parent class of all API resources.
    """

    def __init__(self, api_base_url, logger, orm, zeromq):
        self.logger = logger
        self.api_base_url = api_base_url
        self.orm = orm
        self.zeromq = zeromq
        assert isinstance(self.zeromq, modelstatus.zeromq.ZMQPublisher)


class BaseCollectionResource(BaseResource):
    """
    Parent class of all collection resources.

    Collection resources are used to retrieve lists of resources through GET
    requests, and create new resources through POST requests.
    """

    @falcon.before(modelstatus.utils.deserialize)
    @falcon.after(modelstatus.utils.serialize)
    def on_post(self, req, resp, doc):
        """
        Create a new resource.
        """
        return self._on_post(req, resp, doc)

    def _on_post(self, req, resp, doc):
        """
        Actually create a new resource.

        This is useful for subclasses as they can implement their own
        on_post() and still take advantage of deserialize() and
        serialize(). They can then call this to actually create the
        resource.
        """

        # instantiate the ORM resource from document
        try:
            # always assign automatic primary key on POST
            if 'id' in doc:
                raise AttributeError("The 'id' parameter is not allowed on POST requests to a collection, it will be auto-assigned.")

            # normalize input
            norm_doc = self.normalize_attributes(doc)

            # get the object instance
            orm_resource = self.orm_class(**norm_doc)

        except (TypeError, ValueError, AttributeError), e:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request', unicode(e))

        # add the resource to the transaction manager
        self.orm.add(orm_resource)

        # commit the resource
        try:
            self.orm.commit()
        except sqlalchemy.exc.IntegrityError, e:
            self.orm.rollback()
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid data in request', unicode(e))

        # Here, we are guaranteed to have an object.
        # Fetch it from the database and return it as part of the request.
        object_ = self.orm.query(self.orm_class).filter(self.orm_class.id == orm_resource.id).one()

        # Publish a message through ZeroMQ publisher
        self.zeromq.publish_resource(object_)

        # Flush data to client
        resp.body = object_.serialize()
        resp.status = falcon.HTTP_201

    @falcon.after(modelstatus.utils.serialize)
    def on_get(self, req, resp):
        """
        List all resources, optionally constrained by filtering values.
        """

        try:
            # parse the query string
            query_set = self.orm.query(self.orm_class)
            query_dict = self.parse_query_string(req.query_string)
            filters = self.normalize_filters(query_dict)

            # The query might have to return a limited subset.
            # query_set.filter_by() does not like having limit() called first, cache it here
            limit = filters['limit']
            del filters['limit']

            # Apply ordering (ORDER BY field (ASC|DESC))
            for key, direction in filters['order_by']:
                direction_func = getattr(getattr(self.orm_class, key), direction)
                query_set = query_set.order_by(direction_func())
            del filters['order_by']

            # Apply filters (WHERE ...)
            query_set = query_set.filter_by(**filters)

            # Return a limited subset
            if limit:
                query_set = query_set.limit(limit)

        except (ValueError, sqlalchemy.exc.InvalidRequestError), e:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid query string', unicode(e))

        # Output to client
        resp.body = [object_.serialize() for object_ in query_set]
        resp.status = falcon.HTTP_200

    def parse_query_string(self, query_string):
        """
        Splits an HTTP query string into {key: value} pairs.
        """
        return falcon.util.uri.parse_query_string(query_string)

    def normalize_filters(self, filters):
        """
        Run all filters through filtering functions.
        """
        if not 'order_by' in filters:
            filters['order_by'] = ''
        if not 'limit' in filters:
            filters['limit'] = None

        return self.normalize_attributes(filters)

    def normalize_attributes(self, attr):
        new_attrs = {}
        """
        Run all attributes through normalizing functions if they exist.
        Ensures that both input and output parameters look the same everywhere.
        """
        for key, value in attr.iteritems():
            func_name = 'normalize_' + key
            func = getattr(self, func_name, None)
            if callable(func):
                new_attrs[key] = func(attr[key])
            else:
                new_attrs[key] = value

        return new_attrs

    def normalize_limit(self, value):
        """
        Set up limit value, or throw an error if out of range
        """
        if value is None:
            return value
        value = int(value)
        if value < 1:
            raise ValueError("Limit must be a positive integer and non-zero")
        return value

    def normalize_order_by(self, value):
        """
        Convert a string serialized list of ordering parameters into a list of tuples.

        Example input:  version:desc,created_date:asc
        Example output: [('version', 'desc'), ('created_date', 'asc')]
        """
        order_by = value.split(',')
        order_list = []
        # work with 'field:direction' pairs
        for o in order_by:
            if not o:
                continue
            try:
                (key, order) = o.lower().split(':')
            except ValueError:
                raise ValueError("order declaration must be in the form <field>:<direction>")

            # input validation
            if not order == 'asc' and not order == 'desc':
                raise ValueError("order parameter must be one of 'asc' or 'desc'")
            
            order_list.append((key, order))
        return order_list


class BaseItemResource(BaseResource):
    """
    Parent class of all single-item resources.
    """

    @falcon.after(modelstatus.utils.serialize)
    def on_get(self, req, resp, id):
        """
        Return a single resource
        """

        try:
            object_ = self.orm.query(self.orm_class).filter(self.orm_class.id == id).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise falcon.HTTPNotFound()

        resp.body = object_.serialize()
        resp.status = falcon.HTTP_200
