import threading
import logging
from .models import Job
from . import hadoop_service, spark_service

logger = logging.getLogger(__name__)

# Both engines share the same interface, so launch_job stays a thin dispatcher.
_ENGINES = {
    Job.Engine.HADOOP: hadoop_service,
    Job.Engine.SPARK:  spark_service,
}


def launch_job(job_id: int):
    """Starts the job by launching a thread."""
    try:
        job = Job.objects.get(pk=job_id)
    except Job.DoesNotExist:
        logger.error("launch_job: Job #%d not found", job_id)
        return

    service = _ENGINES.get(job.engine)
    if service is None:
        logger.error("launch_job: engine '%s' not supported", job.engine)
        return

    thread = threading.Thread(
        target=service.submit_and_wait,
        args=(job,),
        name=f"{job.engine}-job-{job_id}",
        daemon=True,
    )
    thread.start()
    logger.info(
        "Thread started for job #%d (thread name: %s)",
        job_id, thread.name,
    )