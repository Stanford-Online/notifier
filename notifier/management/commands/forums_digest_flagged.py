from django.core.management.base import BaseCommand
import logging
from optparse import make_option

from notifier.tasks import do_forums_digests_flagged


logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """
    This Command is used to send a digest of flagged posts to forum moderators.
    """

    help = "Send a digest list of flagged forum posts to each moderator"
    option_list = BaseCommand.option_list + (
        make_option(
            '--posts-file',
            action='store',
            dest='posts_file',
            default=None,
            help='send digests for the specified posts only' +
                ' (defaults to fetching post list via Heroku)',
        ),
        make_option(
            '--is-dry-run',
            action='store_true',
            dest='is_dry_run',
            default=False,
            help='display the course_id and email address' +
                ' for each recipient WITHOUT sending a message',
        ),
    )

    def handle(self, *args, **options):
        """
        Handle a request to send a digest of flagged posts to forum moderators.
        """
        input_file = options.get('posts_file')
        is_dry_run = options.get('is_dry_run')
        do_forums_digests_flagged(input_file, is_dry_run)

