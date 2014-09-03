import tastypie.authorization
import tastypie.resources
import tastypie.exceptions
import core.models
import core.api.validators
import haystack.query
import django.http
import django.conf.urls
import django.core.paginator
import json

from tastypie import fields

# From http://django-tastypie.readthedocs.org/en/latest/cookbook.html#adding-search-functionality
class SearchableResource(tastypie.resources.ModelResource):
    PAGINATOR_PAGE_SIZE = 20
    ALLOWED_QUERY_FIELDS = []

    def prepend_urls(self):
        return [
            django.conf.urls.url(r"^(?P<resource_name>%s)/search/$" % self._meta.resource_name, self.wrap_view('get_search'), name="api_get_search"),
        ]

    def get_search(self, request, **kwargs):
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        self.throttle_check(request)

        # Parameter sanity check
        excess_fields = set(request.GET.keys()) - set(self.ALLOWED_QUERY_FIELDS + ['page'])
        if len(excess_fields) > 0:
            raise tastypie.exceptions.BadRequest("Unsupported search fields: %s; allowed fields are: %s" % (', '.join(excess_fields), ', '.join(self.ALLOWED_QUERY_FIELDS)))

        # Do the query.
        sqs = haystack.query.SearchQuerySet().models(self.Meta.queryset.model).load_all()
        kwargs = {}
        for field in self.ALLOWED_QUERY_FIELDS:
            if field in request.GET:
                kwargs[field] = request.GET.get(field)
        if len(kwargs) > 0:
            sqs = sqs.filter(**kwargs)
        paginator = django.core.paginator.Paginator(sqs, self.PAGINATOR_PAGE_SIZE)

        try:
            page = paginator.page(int(request.GET.get('page', 1)))
        except django.core.paginator.InvalidPage:
            raise django.http.Http404("Sorry, no results on that page.")
        except ValueError:
            raise tastypie.exceptions.BadRequest("Invalid number: %s" % request.GET.get('page'))

        objects = []

        for result in page.object_list:
            bundle = self.build_bundle(obj=result.object, request=request)
            bundle = self.full_dehydrate(bundle)
            objects.append(bundle)

        object_list = {
            'objects': objects,
        }

        self.log_throttled_access(request)
        return self.create_response(request, object_list)

class ModelResource(SearchableResource):
    ALLOWED_QUERY_FIELDS = ['id', 'name']

    id = fields.CharField(attribute='id', help_text='Unique model identification string', blank=False, null=False, unique=True)
    name = fields.CharField(attribute='name', help_text='Descriptive name for the model')

    class Meta:
        queryset = core.models.Model.objects.all()
        authorization = tastypie.authorization.Authorization()
        validation = core.api.validators.ModelValidation()

class DatasetResource(SearchableResource):
    ALLOWED_QUERY_FIELDS = ['model', 'date', 'term']

    model = fields.ToOneField('core.api.resources.ModelResource', 'model', full=True)
    created_at = fields.DateField(attribute='created_at', readonly=True)
    files = fields.ToManyField('core.api.resources.FileResource', 'files', full=True, null=True)

    class Meta:
        queryset = core.models.Dataset.objects.all()
        authorization = tastypie.authorization.Authorization()
        validation = core.api.validators.DatasetValidation()

class FileResource(SearchableResource):
    ALLOWED_QUERY_FIELDS = ['dataset', 'uri']

    dataset = fields.ToOneField('core.api.resources.DatasetResource', 'dataset')
    uri = fields.CharField(attribute='uri')

    class Meta:
        queryset = core.models.File.objects.all()
        authorization = tastypie.authorization.Authorization()
        validation = core.api.validators.FileValidation()
