from django.db import models

class Job(models.Model):
    """it represents a job that can be run on hadoop or spark"""

    class Engine(models.TextChoices):
        HADOOP = 'hadoop'
        SPARK  = 'spark'

    class Status(models.TextChoices):
        SUBMITTED = 'submitted'
        RUNNING   = 'running'
        COMPLETED = 'completed'
        FAILED    = 'failed'

    # parameters choosen by the user in the interface 
    engine          = models.CharField(max_length=10, choices=Engine.choices)
    dataset_path    = models.CharField(max_length=300)
    dataset_size_mb = models.IntegerField(null=True, blank=True)
    window_size     = models.IntegerField(default=2)
    num_reducers    = models.IntegerField(default=2)

    # status 
    status      = models.CharField(max_length=20,
                                   choices=Status.choices,
                                   default=Status.SUBMITTED)
    yarn_app_id = models.CharField(max_length=100, blank=True)
    error_msg   = models.TextField(blank=True)

    # timing 
    submitted_at  = models.DateTimeField(auto_now_add=True)
    started_at    = models.DateTimeField(null=True, blank=True)
    completed_at  = models.DateTimeField(null=True, blank=True)

    # metrics
    execution_time_s = models.FloatField(null=True, blank=True)
    peak_memory_mb   = models.FloatField(null=True, blank=True)
    output_hdfs_path = models.CharField(max_length=300, blank=True)


    class Meta:
        ordering = ['-submitted_at']