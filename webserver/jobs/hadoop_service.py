# jobs/hadoop_service.py
import subprocess
import re
import logging
from pathlib import Path
from django.conf import settings
from django.utils import timezone
from .models import Job

logger = logging.getLogger(__name__)

# Regex to extract the application ID from Hadoop stderr logs
# Es.: "application_1716912345678_0001"
_APP_ID_RE = re.compile(r'application_\d{13}_\d{4,}')


def _build_hadoop_command(job: Job) -> list:
    """Builds the hadoop jar command for the specified job.

    Uses the absolute path to the hadoop binary ($HADOOP_HOME/bin/hadoop)
    to not depend on the PATH configuration in the Docker container.

    Output on HDFS:
      hdfs:///output/job_<id>/word_counts/     - Job 1
      hdfs:///output/job_<id>/co_occurrence/   - Job 2
      hdfs:///output/job_<id>/pmi_output/      - Job 3
    """
    output_base = f"{settings.HDFS_OUTPUT_BASE}/job_{job.id}"
    hadoop_bin = str(Path(settings.HADOOP_HOME) / 'bin' / 'hadoop')

    if job.engine == Job.Engine.HADOOP:
        # Main-Class is declared in the JAR manifest, so we do not pass it on the
        # command line. Passing it would make Driver see 3 args and fail with
        # "Usage: Driver <input_path> <output_base_dir>".
        return [
            hadoop_bin,
            'jar', settings.HADOOP_JAR_PATH,
            job.dataset_path,
            output_base,
        ]

    raise NotImplementedError(f"Engine '{job.engine}' not yet supported.")


def submit_and_wait(job: Job) -> None:
    """Submits the Hadoop job and registers its YARN application id.

    Responsibilities (single-writer for these fields):
      - status = RUNNING + started_at when the subprocess is launched
      - yarn_app_id + output_hdfs_path once the YARN application id is known

    The transition RUNNING -> COMPLETED/FAILED, completed_at and execution_time_s
    are owned exclusively by the poller (sync_job_from_yarn), which reads YARN as
    source of truth. This avoids the previous race where this thread and the
    poller updated the same fields concurrently.

    The only case in which this function still writes a terminal status is a
    purely local failure (subprocess timeout, exception, or no YARN app id
    captured): the poller cannot reconcile such jobs since there is no YARN
    application to query.
    """
    output_base = f"{settings.HDFS_OUTPUT_BASE}/job_{job.id}"

    job.status = Job.Status.RUNNING
    job.started_at = timezone.now()
    job.save(update_fields=['status', 'started_at'])
    logger.info(
        "Job #%d started (engine=%s, dataset=%s)",
        job.id, job.engine, job.dataset_path,
    )

    try:
        cmd = _build_hadoop_command(job)
        logger.debug("Execution: %s", ' '.join(cmd))

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200,
        )

        combined_log = proc.stdout + proc.stderr
        match = _APP_ID_RE.search(combined_log)
        if match:
            job.yarn_app_id = match.group(0)
            update_fields = ['yarn_app_id']
            if proc.returncode == 0:
                job.output_hdfs_path = f"{output_base}/pmi_output"
                update_fields.append('output_hdfs_path')
            job.save(update_fields=update_fields)
            logger.info(
                "Job #%d → YARN app ID: %s (status will be finalized by the poller)",
                job.id, job.yarn_app_id,
            )
            return

        # No YARN app id captured: the poller cannot reconcile this job, so we
        # mark it FAILED locally.
        local_error = combined_log[-2000:].strip() or (
            f"Subprocess exited with code {proc.returncode} without producing a YARN app id"
        )
        logger.error("Job #%d FAILED locally without YARN app id\n%s", job.id, local_error)

    except subprocess.TimeoutExpired:
        local_error = "Job terminated for timeout (limit: 7200 s)"
        logger.error("Job #%d TIMEOUT", job.id)

    except NotImplementedError as exc:
        local_error = str(exc)
        logger.error("Job #%d configuration not supported: %s", job.id, exc)

    except Exception as exc:
        local_error = f"{type(exc).__name__}: {exc}"
        logger.exception("Job #%d unexpected exception", job.id)

    job.status = Job.Status.FAILED
    job.completed_at = timezone.now()
    job.error_msg = local_error
    job.save(update_fields=['status', 'completed_at', 'error_msg'])