from apscheduler.schedulers.blocking import BlockingScheduler
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from notifier.tasks import do_forums_digests
from notifier.tasks import do_forums_digests_flagged

# As it's name implies, when started, this Scheduler will block until forcibly stopped.
sched = BlockingScheduler(standalone=True)


def digest_job():
    do_forums_digests.delay()


def digest_job_flagged():
    """
    Schedule this task via cron job
    """
    do_forums_digests_flagged.delay()

class Command(BaseCommand):

    help = """Start the notifier scheduler.  Important environment settings are:

    BROKER_URL
        Celery broker URL.  Point this where your celery workers look for tasks.

    FORUM_DIGEST_TASK_INTERVAL (optional)
        Number of minutes between digests (int).  Default is 1440.  The value must
        be a factor of 1440.  If 1440, the forums digest job will fire at midnight
        daily.

    FORUM_DIGEST_TASK_INTERVAL_FLAGGED (optional)
        Number of minutes between digests (int).  Default is 0, which disables
        it.  The value must be a factor of 1440.  If 1440, the forums digest
        job will fire at midnight daily.
    """

    def handle(self, *args, **options):
        sched.add_job(digest_job, 'cron', **settings.DIGEST_CRON_SCHEDULE)
        if settings.FORUM_DIGEST_TASK_INTERVAL_FLAGGED > 0:
            sched.add_job(digest_job_flagged, 'cron', **settings.DIGEST_CRON_SCHEDULE_FLAGGED)
        sched.start()
