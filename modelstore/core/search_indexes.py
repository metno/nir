import core.models

from haystack import indexes

class DatasetIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True)
    model = indexes.CharField(model_attr='model__id')
    date = indexes.DateField(model_attr='date')
    term = indexes.IntegerField(model_attr='term')

    def get_model(self):
        return core.models.Dataset

    def index_queryset(self, using=None):
        """Used when the entire index for model is updated."""
        return self.get_model().objects.all()

class ModelIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True)
    id = indexes.CharField(model_attr='id')
    name = indexes.CharField(model_attr='name')

    def get_model(self):
        return core.models.Model

    def index_queryset(self, using=None):
        """Used when the entire index for model is updated."""
        return self.get_model().objects.all()

class FileIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True)
    dataset = indexes.IntegerField(model_attr='dataset__id')
    uri = indexes.CharField(model_attr='uri')

    def get_model(self):
        return core.models.File

    def index_queryset(self, using=None):
        """Used when the entire index for model is updated."""
        return self.get_model().objects.all()
