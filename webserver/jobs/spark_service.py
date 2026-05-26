# jobs/spark_service.py
import logging
import re
import subprocess
import zipfile
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from .models import Job

logger = logging.getLogger(__name__)

# Regex to intercept the YARN application ID
_APP_ID_RE = re.compile(r'application_\d{13}_\d{4,}')

# Temporary ZIP file where the source code of pmi/ is packaged.
# It is regenerated at each submit to ensure that the executors receive the most recent version.
_PMI_ZIP = Path('/tmp/pmi.zip')

_SPARK_CONF = [
    '--executor-memory', '2g',                              # 2 GB heap per executor (datanode YARN cap = 4096 MB, lasciando 1.6 GB di overhead+OS)
    '--executor-cores', '2',
    '--num-executors', '2',
    '--conf', 'spark.dynamicAllocation.enabled=false',     # Disables dynamic allocation to avoid starting the 3rd executor on .136
    '--conf', 'spark.speculation=true',                    # Enables speculative execution to restart slow tasks (stragglers)
    '--conf', 'spark.speculation.quantile=0.5',            # Aggressive speculative threshold (0.5 instead of 0.75 default) for small clusters
    '--conf', 'spark.speculation.interval=10s',            # Speculative check every 10s (default 100ms causes overhead)
    '--conf', 'spark.locality.wait=0s',                    # Removes waiting for data locality (useless on such small clusters)
    '--conf', 'spark.io.compression.codec=zstd',           # Uses Zstandard compression for shuffle (-25% of bytes transferred compared to LZ4)
]


def _build_pmi_zip() -> Path:
    """
    Packs the pmi/ folder recursively into a temporary ZIP archive.
    This step is necessary to enable Spark's '--py-files' option:
    executors running in 'yarn-cluster' mode do not have direct access to the file system
    of the host where the Django server is started. By distributing the ZIP, Spark makes it
    importable on all machines in the YARN cluster.
    """
    pmi_root = Path(settings.SPARK_PMI_DIR)
    if _PMI_ZIP.exists():
        _PMI_ZIP.unlink()
    with zipfile.ZipFile(_PMI_ZIP, 'w', zipfile.ZIP_DEFLATED) as zf:
        for py_file in (pmi_root / 'pmi').rglob('*.py'):
            # Inserisce il file preservando la struttura relativa (es. pmi/jobs.py)
            zf.write(py_file, py_file.relative_to(pmi_root))
    return _PMI_ZIP


def _build_spark_command(job: Job, py_files: Path) -> list:
    """
    Builds the list of parameters for the 'spark-submit' command.
    Command structure:
    spark-submit [spark_options] main.py <dataset_input> <path_output_hdfs> <window_size> <num_reducers>
    """
    output_base = f"{settings.HDFS_OUTPUT_BASE}/job_{job.id}"
    spark_submit = str(Path(settings.SPARK_HOME) / 'bin' / 'spark-submit')
    main_py = str(Path(settings.SPARK_PMI_DIR) / 'main.py')

    return [
        spark_submit,
        '--master', 'yarn',
        '--deploy-mode', 'cluster',
        '--name', f'PMI-Spark-{job.id}',
        *_SPARK_CONF,
        '--py-files', str(py_files),
        main_py,
        job.dataset_path,
        output_base,
        str(job.window_size),
        str(job.num_reducers),
    ]


def submit_and_wait(job: Job) -> None:
    """
    Executes the Spark pipeline within a single YARN application, 
    tracking its status, capturing logs, and handling potential errors synchronously.
    """
    output_base = f"{settings.HDFS_OUTPUT_BASE}/job_{job.id}"

    # 1. Set job status to RUNNING at startup
    job.status = Job.Status.RUNNING
    job.started_at = timezone.now()
    job.save(update_fields=['status', 'started_at'])
    logger.info("Job #%d started (engine=spark, dataset=%s)", job.id, job.dataset_path)

    log_tail: list = []
    app_id: str | None = None
    local_error: str | None = None
    returncode: int | None = None

    try:
        # 2. Creates the ZIP package containing the shared classes/functions
        py_files = _build_pmi_zip()
        cmd = _build_spark_command(job, py_files)
        logger.debug("Execution: %s", ' '.join(cmd))

        # 3. Starts the 'spark-submit' subprocess, capturing output in real time
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Redirects both stderr and stdout to the same stream
            text=True,
            bufsize=1,
        )
        try:
            for line in proc.stdout:
                log_tail.append(line)
                # Keep only the last 200 lines in memory to capture potential final errors
                if len(log_tail) > 200:
                    log_tail = log_tail[-200:]
                
                # Search for the YARN application ID within the logs
                m = _APP_ID_RE.search(line)
                if m and m.group(0) != app_id:
                    app_id = m.group(0)
                    job.yarn_app_id = app_id
                    job.save(update_fields=['yarn_app_id'])
                    logger.info("Job #%d → YARN app ID: %s", job.id, app_id)
            
            # Wait for the process to terminate
            returncode = proc.wait(timeout=7200)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise

        # 4. If the process terminates successfully (exit code 0)
        if returncode == 0:
            job.status = Job.Status.COMPLETED
            job.completed_at = timezone.now()
            job.output_hdfs_path = f"{output_base}/pmi_output"
            job.save(update_fields=['status', 'completed_at', 'output_hdfs_path'])
            logger.info("Job #%d COMPLETED (YARN app: %s)", job.id, app_id)
            return

        # 5. If the exit code indicates a failure
        local_error = ''.join(log_tail).strip()[-2000:] or (
            f"spark-submit exited with code {returncode}"
        )
        logger.error("Job #%d FAILED (exit %s)\n%s", job.id, returncode, local_error)

    except subprocess.TimeoutExpired:
        local_error = "Job terminated for timeout (limit: 7200 s)"
        logger.error("Job #%d TIMEOUT", job.id)

    except Exception as exc:
        local_error = f"{type(exc).__name__}: {exc}"
        logger.exception("Job #%d unexpected exception", job.id)

    # 6. Update status in case of failure or sudden exception
    job.status = Job.Status.FAILED
    job.completed_at = timezone.now()
    job.error_msg = local_error or "Unknown failure"
    job.save(update_fields=['status', 'completed_at', 'error_msg'])
