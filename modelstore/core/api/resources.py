import tastypie.authorization
import tastypie.resources
import core.models
import core.api.validators

from tastypie import fields

class ModelResource(tastypie.resources.ModelResource):
    id = fields.CharField(attribute='id', help_text='Unique model identification string', blank=False, null=False, unique=True)
    name = fields.CharField(attribute='name', help_text='Descriptive name for the model')

    class Meta:
        queryset = core.models.Model.objects.all()
        authorization = tastypie.authorization.Authorization()
        validation = core.api.validators.ModelValidation()

class DatasetResource(tastypie.resources.ModelResource):
    model = fields.ToOneField('core.api.resources.ModelResource', 'model')
    created_at = fields.DateField(attribute='created_at', readonly=True)
    files = fields.ToManyField('core.api.resources.FileResource', 'files', full=True, null=True)

    class Meta:
        queryset = core.models.Dataset.objects.all()
        authorization = tastypie.authorization.Authorization()
        validation = core.api.validators.DatasetValidation()

class FileResource(tastypie.resources.ModelResource):
    dataset = fields.ToOneField('core.api.resources.DatasetResource', 'dataset')
    uri = fields.CharField(attribute='uri')

    class Meta:
        queryset = core.models.File.objects.all()
        authorization = tastypie.authorization.Authorization()
        validation = core.api.validators.FileValidation()
