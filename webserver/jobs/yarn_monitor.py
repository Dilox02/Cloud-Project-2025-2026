# jobs/yarn_monitor.py
import requests
import logging
from django.conf import settings
from django.utils import timezone
from .models import Job

logger = logging.getLogger(__name__)

# Mapping YARN states
_YARN_STATE_MAP = {
    'NEW':        Job.Status.RUNNING,
    'NEW_SAVING': Job.Status.RUNNING,
    'SUBMITTED':  Job.Status.RUNNING,
    'ACCEPTED':   Job.Status.RUNNING,
    'RUNNING':    Job.Status.RUNNING,
    'FINISHED':   Job.Status.COMPLETED,
    'FAILED':     Job.Status.FAILED,
    'KILLED':     Job.Status.FAILED,
}


def get_yarn_app_info(yarn_app_id: str):
    """Retrieves application information from the YARN ResourceManager.

    Returns the 'app' dictionary in case of success, {} in case of error.
    """
    rm_url = getattr(settings, 'YARN_RESOURCEMANAGER_URL', 'http://localhost:8088')
    url = f"{rm_url}/ws/v1/cluster/apps/{yarn_app_id}"

    try:
        resp = requests.get(url, timeout=10, headers={'Accept': 'application/json'})
        resp.raise_for_status()
        return resp.json().get('app', {})
    except requests.RequestException as exc:
        logger.warning("YARN REST API not reachable for %s: %s", yarn_app_id, exc)
        return {}


def sync_job_from_yarn(job: Job):
    """Updates the Job record with the most recent information from YARN.

    Returns True if at least one field has changed, False otherwise.
    """
    if not job.yarn_app_id:
        return False

    info = get_yarn_app_info(job.yarn_app_id)
    if not info:
        return False

    yarn_state  = info.get('state', '')
    yarn_final  = info.get('finalStatus', '')
    elapsed_ms  = info.get('elapsedTime', 0)#total execution time in ms
    mem_seconds = info.get('memorySeconds', 0) #memory allocated x time in seconds (belongs to YARN :P) 
    #to obtain avg memory we divide mem_seconds by the time in seconds

    new_status = _YARN_STATE_MAP.get(yarn_state, Job.Status.RUNNING)

    if yarn_state == 'FINISHED' and yarn_final == 'FAILED':
        new_status = Job.Status.FAILED

    update_fields = []

    if job.status != new_status:
        job.status = new_status
        update_fields.append('status')

        if new_status in (Job.Status.COMPLETED, Job.Status.FAILED):
            job.completed_at = timezone.now()
            update_fields.append('completed_at')

    if elapsed_ms and job.execution_time_s is None:
        job.execution_time_s = round(elapsed_ms / 1000.0, 2) #ms to s
        update_fields.append('execution_time_s')

    if mem_seconds and elapsed_ms:
        job.peak_memory_mb = round(mem_seconds / max(elapsed_ms / 1000.0, 1), 1)#avg memory in MB
        update_fields.append('peak_memory_mb')

    if update_fields:
        job.save(update_fields=update_fields)
        logger.info(
            "Job #%d: state=%s (fields: %s)",
            job.id, new_status, update_fields,
        )
        return True

    return False
