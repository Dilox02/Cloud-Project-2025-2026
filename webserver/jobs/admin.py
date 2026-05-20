from django.contrib import admin
from .models import Job


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display  = ('id', 'engine', 'status', 'dataset_path',
                     'submitted_at', 'execution_time_s')
    list_filter   = ('engine', 'status')
    search_fields = ('dataset_path', 'yarn_app_id')
    readonly_fields = (
        'status', 'yarn_app_id', 'error_msg',
        'submitted_at', 'started_at', 'completed_at',
        'execution_time_s', 'peak_memory_mb', 'output_hdfs_path',
    )
