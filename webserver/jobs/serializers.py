from rest_framework import serializers
from .models import Job

"""Django REST Framework serializers act as a bidirectional translator between Python data and JSON. 
They handle data serialization for outgoing responses, validation, and deserialization for incoming API requests.
"""
class JobSerializer(serializers.ModelSerializer):
    """Complete serializer,it's used mainly when we want to retrieve all the job info with a GET request"""

    duration_s = serializers.SerializerMethodField()
    live       = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = [
            'id',
            'engine',
            'dataset_path',
            'dataset_size_mb',
            'window_size',
            'num_reducers',
            'status',
            'yarn_app_id',
            'error_msg',
            'submitted_at',
            'started_at',
            'completed_at',
            'execution_time_s',
            'peak_memory_mb',
            'output_hdfs_path',
            'duration_s',
            'live',
        ]


    def get_duration_s(self, obj):
        """it calculates the duration of the job if it's completed"""
        if obj.started_at and obj.completed_at:
            return round((obj.completed_at - obj.started_at).total_seconds(), 2)
        return None

    def get_live(self, obj):
        """Live progress from YARN (only for RUNNING jobs with a known app id)."""
        if obj.status == Job.Status.RUNNING and obj.yarn_app_id:
            from .yarn_monitor import get_live_progress
            data = get_live_progress(obj.yarn_app_id, obj.engine)
            return data or None
        return None


class JobCreateSerializer(serializers.ModelSerializer):
    """Minimal serializer,it's used when we want to create a new job with a POST request"""

    class Meta:
        model = Job
        fields = ['engine', 'dataset_path', 'dataset_size_mb', 'window_size', 'num_reducers']

    def validate_dataset_path(self, value):
        if not value.startswith('hdfs://') and not value.startswith('/'):
            raise serializers.ValidationError(
                "dataset_path must be an HDFS path (es. hdfs:///input/corpus) "
                "or an absolute path."
            )
        return value

    def validate_window_size(self, value):
        if not (1 <= value <= 10):
            raise serializers.ValidationError("window_size must be between 1 and 10.")
        return value

    def validate_num_reducers(self, value):
        if not (1 <= value <= 64):
            raise serializers.ValidationError("num_reducers must be between 1 and 64.")
        return value


class JobStatusSerializer(serializers.ModelSerializer):
    """Lightweight serializer,it's used for polling the job status"""

    live = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = [
            'id',
            'status',
            'yarn_app_id',
            'submitted_at',
            'started_at',
            'completed_at',
            'execution_time_s',
            'output_hdfs_path',
            'error_msg',
            'live',
        ]

    def get_live(self, obj):
        if obj.status == Job.Status.RUNNING and obj.yarn_app_id:
            from .yarn_monitor import get_live_progress
            data = get_live_progress(obj.yarn_app_id, obj.engine)
            return data or None
        return None