# jobs/runner.py
import threading
import logging
from .models import Job
from .hadoop_service import submit_and_wait

logger = logging.getLogger(__name__)


def launch_job(job_id: int):
    """Starts the job by launching a thread.
    """
    try:
        job = Job.objects.get(pk=job_id)
    except Job.DoesNotExist:
        logger.error("launch_job: Job #%d not found", job_id)
        return

    thread = threading.Thread(
        target=submit_and_wait,
        args=(job,),
        name=f"hadoop-job-{job_id}",
        daemon=True,
    )
    thread.start()
    logger.info(
        "Thread started for job #%d (thread name: %s)",
        job_id, thread.name,
    )