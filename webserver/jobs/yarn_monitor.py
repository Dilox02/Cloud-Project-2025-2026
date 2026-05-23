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

# Pipeline stage index, used to compute overall pipeline progress.
_STAGE_INDEX = {
    'Word Count':             1,
    'Co-occurrence Stripes':  2,
    'PMI Computation':        3,
}
_PIPELINE_STAGES = 3


def get_live_progress(yarn_app_id: str):
    """Return live progress info for the currently running YARN app.

    Single-stage progress comes straight from YARN (`progress` field, 0-100).
    Pipeline progress combines the stage index and the current stage progress:
      ((stage_index - 1) * 100 + yarn_progress) / 3
    """
    if not yarn_app_id:
        return {}
    info = get_yarn_app_info(yarn_app_id)
    if not info:
        return {}
    name = info.get('name', '')
    yarn_progress = float(info.get('progress', 0) or 0)
    stage_index = _STAGE_INDEX.get(name)
    pipeline_progress = None
    if stage_index:
        pipeline_progress = round(((stage_index - 1) * 100 + yarn_progress) / _PIPELINE_STAGES, 1)
    return {
        'progress':          round(yarn_progress, 1),
        'stage_name':        name,
        'stage_index':       stage_index,
        'stage_total':       _PIPELINE_STAGES,
        'pipeline_progress': pipeline_progress,
        'yarn_state':        info.get('state', ''),
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


def _find_latest_pipeline_app(submitted_at) -> str:
    """Find the most recent pipeline YARN app started after submitted_at.

    Assumes pipelines are submitted sequentially: with overlapping pipelines
    this could return an app belonging to a different job. Good enough on a
    cluster sized for one pipeline at a time (our case).
    """
    rm_url = getattr(settings, 'YARN_RESOURCEMANAGER_URL', 'http://localhost:8088')
    started_ms = int(submitted_at.timestamp() * 1000)
    url = f"{rm_url}/ws/v1/cluster/apps?startedTimeBegin={started_ms}"
    try:
        resp = requests.get(url, timeout=10, headers={'Accept': 'application/json'})
        resp.raise_for_status()
        apps = (resp.json().get('apps') or {}).get('app', []) or []
    except requests.RequestException as exc:
        logger.warning("YARN REST API not reachable: %s", exc)
        return ''
    pipeline = [a for a in apps if a.get('name') in _STAGE_INDEX]
    if not pipeline:
        return ''
    pipeline.sort(key=lambda a: a.get('startedTime', 0))
    return pipeline[-1].get('id', '')


def sync_job_from_yarn(job: Job):
    """Updates the Job record with the most recent information from YARN.

    Returns True if at least one field has changed, False otherwise.

    Acts as the safety-net of the lifecycle: the poller is the authoritative
    source of the COMPLETED/FAILED transition. submit_and_wait only sets
    yarn_app_id and intermediate state during a live run; if its thread dies
    (e.g. Django StatReloader restart) the poller still drives the job to
    its terminal state by:
      - actively switching yarn_app_id to the latest pipeline app on YARN
        when the current one is FINISHED but is an intermediate stage,
      - marking COMPLETED only when the *last* stage (PMI Computation)
        reports FINISHED/SUCCEEDED.
    """
    if not job.yarn_app_id:
        return False

    info = get_yarn_app_info(job.yarn_app_id)
    if not info:
        return False

    update_fields = []
    yarn_name  = info.get('name', '')
    yarn_state = info.get('state', '')

    # Safety-net: if the tracked app finished but wasn't the final PMI stage,
    # hop yarn_app_id to the latest pipeline app started after job.submitted_at.
    if yarn_state == 'FINISHED' and yarn_name != 'PMI Computation':
        newer = _find_latest_pipeline_app(job.submitted_at)
        if newer and newer != job.yarn_app_id:
            logger.info("Job #%d: poller advancing yarn_app_id %s -> %s",
                        job.id, job.yarn_app_id, newer)
            job.yarn_app_id = newer
            update_fields.append('yarn_app_id')
            refreshed = get_yarn_app_info(newer)
            if refreshed:
                info       = refreshed
                yarn_name  = info.get('name', '')
                yarn_state = info.get('state', '')

    yarn_final  = info.get('finalStatus', '')
    elapsed_ms  = info.get('elapsedTime', 0)#total execution time in ms
    mem_seconds = info.get('memorySeconds', 0) #memory allocated x time in seconds (belongs to YARN :P)
    #to obtain avg memory we divide mem_seconds by the time in seconds

    new_status = _YARN_STATE_MAP.get(yarn_state, Job.Status.RUNNING)

    if yarn_state == 'FINISHED' and yarn_final == 'FAILED':
        new_status = Job.Status.FAILED

    # Only the last stage advances the job to COMPLETED; intermediate FINISHEDs
    # keep the job RUNNING (the safety-net above will eventually hop to PMI).
    if new_status == Job.Status.COMPLETED and yarn_name != 'PMI Computation':
        new_status = Job.Status.RUNNING

    if job.status != new_status:
        job.status = new_status
        update_fields.append('status')

        if new_status in (Job.Status.COMPLETED, Job.Status.FAILED):
            job.completed_at = timezone.now()
            update_fields.append('completed_at')

        if new_status == Job.Status.COMPLETED and not job.output_hdfs_path:
            job.output_hdfs_path = f"{settings.HDFS_OUTPUT_BASE}/job_{job.id}/pmi_output"
            update_fields.append('output_hdfs_path')

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
