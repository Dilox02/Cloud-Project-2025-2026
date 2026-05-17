from django.contrib import admin
from .models import Job

@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ('id', 'engine', 'status', 'submitted_at', 'execution_time_s')
    list_filter = ('engine', 'status')
    search_fields = ('dataset_path', 'yarn_app_id')
