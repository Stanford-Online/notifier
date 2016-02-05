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

    @patch('notifier.user.get_moderators', return_value=(usern(i) for i in xrange(10)))
    @patch('notifier.pull.get_flagged_threads', return_value=make_flagged_threads())
    def test_forums_digests_flagged_dry(self, _get_threads, _get_moderators):
        """
        Test that command with dry run option works.
        """
        out = sys.stdout
        sys.stdout = StringIO()
        management.call_command('forums_digest_flagged', is_dry_run=True)
        output = sys.stdout.getvalue()  # pylint: disable=no-member
        for i in xrange(10):
            self.assertIn("To: {}".format(usern(i)['email']), output)
        self.assertIn(self.test_course, output)
        sys.stdout.close()
        sys.stdout = out
