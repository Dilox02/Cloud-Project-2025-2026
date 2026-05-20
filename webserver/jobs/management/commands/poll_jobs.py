# jobs/management/commands/poll_jobs.py
import time
import logging
from django.core.management.base import BaseCommand
from jobs.models import Job
from jobs.yarn_monitor import sync_job_from_yarn

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        'Interroga YARN REST API per aggiornare lo stato dei job in esecuzione. '
        'Eseguire una volta dopo ogni riavvio del server, oppure in loop come daemon.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval',
            type=int,
            default=0,
            metavar='SECONDI',
            help='Se > 0, esegue in loop continuo ogni N secondi (Ctrl+C per fermare).',
        )

    def handle(self, *args, **options):
        interval = options['interval']

        if interval > 0:
            self.stdout.write(
                self.style.SUCCESS(f'[poll_jobs] Avviato in loop ogni {interval}s — Ctrl+C per fermare')
            )
            try:
                while True:
                    self._poll_once()
                    time.sleep(interval)
            except KeyboardInterrupt:
                self.stdout.write('\n[poll_jobs] Fermato.')
        else:
            self._poll_once()

    def _poll_once(self):
        # Solo job con yarn_app_id noto (esclude quelli appena creati)
        running = Job.objects.filter(
            status=Job.Status.RUNNING,
            yarn_app_id__gt='',
        )

        if not running.exists():
            self.stdout.write('[poll_jobs] Nessun job in esecuzione con YARN app ID.')
            return

        self.stdout.write(f'[poll_jobs] Controllo {running.count()} job in esecuzione...')

        for job in running:
            changed = sync_job_from_yarn(job)

            if job.status == Job.Status.COMPLETED:
                label = self.style.SUCCESS(job.status)
            elif job.status == Job.Status.FAILED:
                label = self.style.ERROR(job.status)
            else:
                label = job.status

            suffix = '  ← aggiornato' if changed else ''
            self.stdout.write(f'  Job #{job.id} ({job.yarn_app_id}): {label}{suffix}')