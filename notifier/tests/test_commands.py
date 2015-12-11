"""
"""
from cStringIO import StringIO
import datetime
import json
from os.path import dirname, join
import sys

from django.conf import settings
from django.core import management
from django.test import TestCase
from django.test.utils import override_settings
from mock import patch, Mock

from notifier.management.commands import forums_digest
from notifier.tests.test_tasks import make_flagged_threads, usern


class CommandsTestCase(TestCase):
    """
    Test notifier management commands
    """
    def setUp(self):
        self.test_course = 'org/course/run'

    @override_settings(CELERY_EAGER_PROPAGATES_EXCEPTIONS=True,
                       CELERY_ALWAYS_EAGER=True,
                       BROKER_BACKEND='memory',)
    def test_forums_digest(self):
        pass

    def test_forums_digests_flagged_dry(self):
        """
        Test that command with dry run option works.
        """
        test_threads = make_flagged_threads()
        test_moderators = (usern(i) for i in xrange(10))
        with patch('notifier.management.commands.forums_digest_flagged.get_flagged_threads', return_value=test_threads), \
                patch('notifier.management.commands.forums_digest_flagged.get_moderators', return_value=test_moderators):
            out = sys.stdout
            sys.stdout = StringIO()
            management.call_command('forums_digest_flagged', is_dry_run=True)
            output = sys.stdout.getvalue()
            for i in xrange(10):
                self.assertIn("To: {}".format(usern(i)['email']), output)
            self.assertIn(self.test_course, output)
            sys.stdout.close()
            sys.stdout = out
