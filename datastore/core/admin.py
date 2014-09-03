from django.contrib import admin

import core.models

class FileAdminInline(admin.TabularInline):
    model = core.models.File

class DatasetAdmin(admin.ModelAdmin):
    readonly_fields = ('created_at', 'modified_at',)
    inlines = (FileAdminInline,)

admin.site.register(core.models.Model)
admin.site.register(core.models.Dataset, DatasetAdmin)
admin.site.register(core.models.File)
