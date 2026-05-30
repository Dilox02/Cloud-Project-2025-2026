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
        # command line. Args: <input> <output_base> <window_size> <num_reducers>.
        return [
            hadoop_bin,
            'jar', settings.HADOOP_JAR_PATH,
            job.dataset_path,
            output_base,
            str(job.window_size),
            str(job.num_reducers),
        ]

    raise NotImplementedError(f"Engine '{job.engine}' not yet supported.")


def submit_and_wait(job: Job) -> None:
    """Submits the Hadoop pipeline and tracks the job lifecycle in DB.

    The Driver runs 3 MapReduce jobs sequentially (word count -> stripes -> PMI).
    We stream the subprocess output line-by-line and keep yarn_app_id pointed at
    the *currently running* app, so the poller always monitors the live stage.

    Terminal transition COMPLETED/FAILED is set here when the subprocess returns
    cleanly. If this thread is killed mid-run (e.g. Django StatReloader restart),
    the poller's safety-net in sync_job_from_yarn reconciles the job from YARN
    (finds pipeline apps started after job.submitted_at).
    """
    output_base = f"{settings.HDFS_OUTPUT_BASE}/job_{job.id}"

    job.status = Job.Status.RUNNING
    job.started_at = timezone.now()
    job.save(update_fields=['status', 'started_at'])
    logger.info(
        "Job #%d started (engine=%s, dataset=%s)",
        job.id, job.engine, job.dataset_path,
    )

    log_tail: list = []
    last_app_id: str | None = None
    local_error: str | None = None
    returncode: int | None = None

    try:
        cmd = _build_hadoop_command(job)
        logger.debug("Execution: %s", ' '.join(cmd))

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        try:
            for line in proc.stdout:
                log_tail.append(line)
                if len(log_tail) > 200:
                    log_tail = log_tail[-200:]
                m = _APP_ID_RE.search(line)
                if m and m.group(0) != last_app_id:
                    last_app_id = m.group(0)
                    job.yarn_app_id = last_app_id
                    job.save(update_fields=['yarn_app_id'])
                    logger.info("Job #%d → YARN app ID: %s", job.id, last_app_id)
            returncode = proc.wait(timeout=7200)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise

        if returncode == 0:
            job.status = Job.Status.COMPLETED
            job.completed_at = timezone.now()
            job.output_hdfs_path = f"{output_base}/pmi_output"
            job.save(update_fields=['status', 'completed_at', 'output_hdfs_path'])
            logger.info("Job #%d COMPLETED (last YARN app: %s)", job.id, last_app_id)
            from .yarn_monitor import sync_job_from_yarn
            sync_job_from_yarn(job)
            return

        local_error = ''.join(log_tail).strip()[-2000:] or (
            f"Subprocess exited with code {returncode}"
        )
        logger.error("Job #%d FAILED (exit %s)\n%s", job.id, returncode, local_error)

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
    job.error_msg = local_error or "Unknown failure"
    job.save(update_fields=['status', 'completed_at', 'error_msg'])