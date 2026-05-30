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
_SPARK_TOTAL_JOBS = 3  # actions emitted by spark-pmi/pmi/pipeline.py: sum, collectAsMap, save_pmi


def get_live_progress(yarn_app_id: str, engine: str = Job.Engine.HADOOP):
    """Dispatches to the engine-specific progress fetcher."""
    if not yarn_app_id:
        return {}
    if engine == Job.Engine.SPARK:
        return _get_spark_progress(yarn_app_id)
    return _get_hadoop_progress(yarn_app_id)


def _get_hadoop_progress(yarn_app_id: str):
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


def _get_spark_progress(yarn_app_id: str):
    rm_url = getattr(settings, 'YARN_RESOURCEMANAGER_URL', 'http://localhost:8088')
    base = f"{rm_url}/proxy/{yarn_app_id}/api/v1/applications/{yarn_app_id}"
    try:
        spark_jobs = requests.get(f"{base}/jobs", timeout=5,
                                  headers={'Accept': 'application/json'}).json()
        stages = requests.get(f"{base}/stages", timeout=5,
                              headers={'Accept': 'application/json'}).json()
    except requests.RequestException as exc:
        logger.warning("Spark REST API not reachable for %s: %s", yarn_app_id, exc)
        return {}

    completed = sum(1 for j in spark_jobs if j.get('status') == 'SUCCEEDED')
    running   = next((j for j in spark_jobs if j.get('status') == 'RUNNING'), None)
    total = max(len(spark_jobs), _SPARK_TOTAL_JOBS)

    job_progress = 0.0
    stage_name = 'idle'
    if running:
        n_tasks = running.get('numTasks', 0)
        n_done  = running.get('numCompletedTasks', 0)
        if n_tasks:
            job_progress = round(n_done / n_tasks * 100, 1)
        active = next((s for s in stages if s.get('status') == 'ACTIVE'), None)
        src = active or running
        stage_name = (src.get('name') or '').split(' at ')[0] or 'running'

    pipeline_progress = round((completed * 100 + job_progress) / total, 1)
    return {
        'progress':          job_progress,
        'stage_name':        stage_name,
        'stage_index':       completed + (1 if running else 0),
        'stage_total':       total,
        'pipeline_progress': pipeline_progress,
        'yarn_state':        'RUNNING',
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


def _accumulate_stage(job: Job, elapsed_ms: int, mem_seconds: int, update_fields: set):
    """Track peak average memory across YARN stages (max, idempotent)."""
    if mem_seconds and elapsed_ms:
        stage_avg_mb = round(mem_seconds / max(elapsed_ms / 1000.0, 1), 1)
        if job.peak_memory_mb is None or stage_avg_mb > job.peak_memory_mb:
            job.peak_memory_mb = stage_avg_mb
            update_fields.add('peak_memory_mb')


def sync_job_from_yarn(job: Job):
    """Updates the Job record with the most recent information from YARN.

    Returns True if at least one field has changed, False otherwise.

    Drives job lifecycle and updates peak_memory_mb across stages.
    execution_time_s and duration_s are computed by the serializer.

    Hadoop: captures memorySeconds of each stage on hop, keeps the max
    average across stages as peak_memory_mb.
    Spark: single YARN app, peak_memory_mb set on completion.
    """
    if not job.yarn_app_id:
        return False

    info = get_yarn_app_info(job.yarn_app_id)
    if not info:
        return False

    update_fields = set()
    yarn_name  = info.get('name', '')
    yarn_state = info.get('state', '')
    is_hadoop  = job.engine == Job.Engine.HADOOP

    # Stage-hopping (Hadoop only): before advancing to the next YARN app,
    # fold the completed stage's metrics into the running totals.
    if is_hadoop and yarn_state == 'FINISHED' and yarn_name != 'PMI Computation':
        stage_elapsed_ms  = info.get('elapsedTime', 0) or 0
        stage_mem_seconds = info.get('memorySeconds', 0) or 0
        _accumulate_stage(job, stage_elapsed_ms, stage_mem_seconds, update_fields)

        newer = _find_latest_pipeline_app(job.submitted_at)
        if newer and newer != job.yarn_app_id:
            logger.info("Job #%d: poller advancing yarn_app_id %s -> %s",
                        job.id, job.yarn_app_id, newer)
            job.yarn_app_id = newer
            update_fields.add('yarn_app_id')
            refreshed = get_yarn_app_info(newer)
            if refreshed:
                info       = refreshed
                yarn_name  = info.get('name', '')
                yarn_state = info.get('state', '')

    yarn_final  = info.get('finalStatus', '')
    elapsed_ms  = info.get('elapsedTime', 0) or 0
    mem_seconds = info.get('memorySeconds', 0) or 0

    new_status = _YARN_STATE_MAP.get(yarn_state, Job.Status.RUNNING)

    if yarn_state == 'FINISHED' and yarn_final == 'FAILED':
        new_status = Job.Status.FAILED

    if is_hadoop and new_status == Job.Status.COMPLETED and yarn_name != 'PMI Computation':
        new_status = Job.Status.RUNNING

    if job.status != new_status:
        job.status = new_status
        update_fields.add('status')

        if new_status in (Job.Status.COMPLETED, Job.Status.FAILED):
            job.completed_at = timezone.now()
            update_fields.add('completed_at')

        if new_status == Job.Status.COMPLETED and not job.output_hdfs_path:
            job.output_hdfs_path = f"{settings.HDFS_OUTPUT_BASE}/job_{job.id}/pmi_output"
            update_fields.add('output_hdfs_path')

    # On completion, fold the final stage's metrics and seal the values.
    if new_status == Job.Status.COMPLETED:
        _accumulate_stage(job, elapsed_ms, mem_seconds, update_fields)

    if update_fields:
        job.save(update_fields=list(update_fields))
        logger.info("Job #%d: state=%s (fields: %s)", job.id, new_status, update_fields)
        return True

    return False
