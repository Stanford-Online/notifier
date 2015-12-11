"""
"""
from contextlib import nested
import datetime
import json
from os.path import dirname, join

from boto.ses.exceptions import SESMaxSendingRateExceededError
from django.conf import settings
from django.core import mail as djmail
from django.test import TestCase
from django.test.utils import override_settings
from mock import patch, Mock

from notifier.tasks import generate_and_send_digests, do_forums_digests
from notifier.tasks import generate_and_send_digests_flagged
from notifier.tasks import do_forums_digests_flagged
from notifier.pull import process_cs_response, CommentsServiceException
from notifier.user import UserServiceException, DIGEST_NOTIFICATION_PREFERENCE_KEY
from .utils import make_user_info


TEST_COURSE_ID = 'org/course/run'
TEST_COMMENTABLE = 'test_commentable'

# fixture data helper
usern = lambda n: {
    'name': 'user%d' % n,
    'id': n,
    'email': 'user%d@dummy.edu' %n,
    'preferences': {
        DIGEST_NOTIFICATION_PREFERENCE_KEY: 'pref%d' % n,
    },
    'course_info': {

    }
}


def make_flagged_threads():
    """Makes test fixture response from flagged threads API."""
    return [
        {
            'course_id': TEST_COURSE_ID,
            'commentable_id': TEST_COMMENTABLE,
            'comment_thread_id': i,
        }
        for i in xrange(1, 6)
    ]


def make_messages(count_messages=5, count_posts=10):
    """
    Create sample messages for testing

    Args:
        count_messages (int): number of total messages to be made
        count_posts (int): number of posts per message

    Returns:
        messages (list): contains dicts of messages
    """
    return [
        {
            'course_id': TEST_COURSE_ID,
            'recipient': usern(i),
            'threads': [
                '{base_url}/courses/{course_id}/discussion/forum/{commentable_id}/threads/{comment_thread_id}'.format(
                    base_url=settings.LMS_URL_BASE,
                    course_id=TEST_COURSE_ID,
                    commentable_id=TEST_COMMENTABLE,
                    comment_thread_id=j,
                )
                for j in xrange(1, count_posts + 1)
            ],
        }
        for i in xrange(count_messages)
    ]


@override_settings(CELERY_EAGER_PROPAGATES_EXCEPTIONS=True,
                   CELERY_ALWAYS_EAGER=True,
                   EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend',
                   BROKER_BACKEND='memory',)
class TasksTestCase(TestCase):

    """
    """

    def _check_message(self, user, digest, message):
        actual_text = message.body
        actual_html, mime_type = message.alternatives[0]
        self.assertEqual(mime_type, 'text/html')

        self.assertEqual(message.from_email, settings.FORUM_DIGEST_EMAIL_SENDER)
        self.assertEqual(message.to, [user['email']])
        self.assertEqual(message.subject, settings.FORUM_DIGEST_EMAIL_SUBJECT)

        self.assertTrue(user['name'] in actual_text)
        self.assertTrue(settings.FORUM_DIGEST_EMAIL_TITLE in actual_text)
        self.assertTrue(settings.FORUM_DIGEST_EMAIL_DESCRIPTION in actual_text)

        self.assertTrue(user['name'] in actual_html)
        self.assertTrue(settings.FORUM_DIGEST_EMAIL_TITLE in actual_html)
        self.assertTrue(settings.FORUM_DIGEST_EMAIL_DESCRIPTION in actual_html)

        for course in digest.courses:
            self.assertTrue(course.title in actual_text)
            self.assertTrue(course.title in actual_html)
            for thread in course.threads:
                self.assertTrue(thread.title in actual_text)
                self.assertTrue(thread.title in actual_html)
                for item in thread.items:
                    self.assertTrue(item.body in actual_text)
                    self.assertTrue(item.body in actual_html)

    def _process_cs_response_with_user_info(self, data):
        mock_user_info = make_user_info(data)
        return process_cs_response(data, mock_user_info)

    def test_generate_and_send_digests(self):
        """
        """
        data = json.load(
            open(join(dirname(__file__), 'cs_notifications.result.json')))

        user_id, digest = self._process_cs_response_with_user_info(data).next()
        user = usern(10)
        with patch('notifier.tasks.generate_digest_content', return_value=[(user_id, digest)]) as p:

            # execute task
            task_result = generate_and_send_digests.delay(
                [user],
                datetime.datetime.now(),
                datetime.datetime.now())
            self.assertTrue(task_result.successful())

            # message was sent
            self.assertTrue(hasattr(djmail, 'outbox'))
            self.assertEqual(1, len(djmail.outbox))

            # message has expected to, from, subj, and content
            self._check_message(user, digest, djmail.outbox[0])

    @override_settings(EMAIL_REWRITE_RECIPIENT='rewritten-address@domain.org')
    def test_generate_and_send_digests_rewrite_recipient(self):
        """
        """
        data = json.load(
            open(join(dirname(__file__), 'cs_notifications.result.json')))

        with patch(
            'notifier.tasks.generate_digest_content',
            return_value=self._process_cs_response_with_user_info(data)
        ):
            # execute task
            task_result = generate_and_send_digests.delay(
                (usern(n) for n in xrange(2, 11)), datetime.datetime.now(), datetime.datetime.now())
            self.assertTrue(task_result.successful())

            # all messages were sent
            self.assertTrue(hasattr(djmail, 'outbox'))
            self.assertEqual(9, len(djmail.outbox))

            # all messages' email addresses were rewritten
            for message in djmail.outbox:
                self.assertEqual(message.to, ['rewritten-address@domain.org'])

    def test_generate_and_send_digests_retry_ses(self):
        """
        """
        data = json.load(
            open(join(dirname(__file__), 'cs_notifications.result.json')))

        with patch(
            'notifier.tasks.generate_digest_content',
            return_value=list(self._process_cs_response_with_user_info(data))
        ):
            # setting this here because override_settings doesn't seem to
            # work on celery task configuration decorators
            expected_num_tries = 1 + settings.FORUM_DIGEST_TASK_MAX_RETRIES
            mock_backend = Mock(name='mock_backend', send_messages=Mock(
                side_effect=SESMaxSendingRateExceededError(400, 'Throttling')))
            with patch('notifier.connection_wrapper.dj_get_connection', return_value=mock_backend) as p2:
                # execute task - should fail, retry twice and still fail, then
                # give up
                try:
                    generate_and_send_digests.delay(
                        [usern(n) for n in xrange(2, 11)], datetime.datetime.now(), datetime.datetime.now())
                except SESMaxSendingRateExceededError as e:
                    self.assertEqual(
                        mock_backend.send_messages.call_count,
                        expected_num_tries)
                else:
                    # should have raised
                    self.fail('task did not retry twice before giving up')

    def test_generate_and_send_digests_retry_cs(self):
        """
        """
        with patch(
            'notifier.tasks.generate_digest_content',
            side_effect=CommentsServiceException('timed out')
        ) as mock_cs_call:
            # setting this here because override_settings doesn't seem to
            # work on celery task configuration decorators
            expected_num_tries = 1 + settings.FORUM_DIGEST_TASK_MAX_RETRIES
            try:
                generate_and_send_digests.delay(
                    [usern(n) for n in xrange(2, 11)], datetime.datetime.now(), datetime.datetime.now())
            except CommentsServiceException as e:
                self.assertEqual(mock_cs_call.call_count, expected_num_tries)
            else:
                # should have raised
                self.fail('task did not retry twice before giving up')

    def test_generate_and_send_digests_flagged(self):
        """
        Test that we can generate and send flagged forum digest emails.
        """
        messages = make_messages()

        task_result = generate_and_send_digests_flagged.delay(messages)
        self.assertTrue(task_result.successful())

        self.assertTrue(hasattr(djmail, 'outbox'))
        self.assertEqual(5, len(djmail.outbox))

        for i, message in enumerate(djmail.outbox):
            self.assertEqual([messages[i]['recipient']['email']], message.to)
            for thread in messages[i]['threads']:
                self.assertIn(thread, message.body)

    def test_generate_and_send_digests_flagged_partial_retry(self):
        """
        Test that partial retries are not attempted
        """
        def side_effect(msgs):
            msgs[0].extra_headers['status'] = 200
            raise SESMaxSendingRateExceededError(400, 'Throttling')

        messages = make_messages()
        mock_backend = Mock(
            name='mock_backend',
            send_messages=Mock(
                side_effect=side_effect,
            ),
        )
        with patch('notifier.connection_wrapper.dj_get_connection', return_value=mock_backend):
            # execute task - should fail, and not retry
            try:
                generate_and_send_digests_flagged.delay(messages)
            except SESMaxSendingRateExceededError as e:
                self.assertEqual(mock_backend.send_messages.call_count, 1)

    def test_generate_and_send_digests_flagged_retry_ses(self):
        """
        Test that retries are attempted
        """
        messages = make_messages()

        # setting this here because override_settings doesn't seem to
        # work on celery task configuration decorators
        expected_num_tries = 1 + settings.FORUM_DIGEST_TASK_MAX_RETRIES
        mock_backend = Mock(name='mock_backend', send_messages=Mock(
            side_effect=SESMaxSendingRateExceededError(400, 'Throttling')))
        with patch('notifier.connection_wrapper.dj_get_connection', return_value=mock_backend):
            # execute task - should fail, retry twice and still fail, then
            # give up
            try:
                generate_and_send_digests_flagged.delay(messages)
            except SESMaxSendingRateExceededError as e:
                self.assertEqual(
                    mock_backend.send_messages.call_count,
                    expected_num_tries,
                )

    @override_settings(FORUM_DIGEST_TASK_BATCH_SIZE=9)
    def test_do_forums_digests_flagged(self):
        """
        Test that we can send forum digests for flagged posts
        """
        test_threads = make_flagged_threads()
        test_moderators = (usern(i) for i in xrange(10))

        last_message_batch = [
            {
                'course_id': TEST_COURSE_ID,
                'recipient': usern(9),
                'threads': [
                    '{base_url}/courses/{course_id}/discussion/forum/{commentable_id}/threads/{comment_thread_id}'.format(
                        base_url=settings.LMS_URL_BASE,
                        course_id=TEST_COURSE_ID,
                        commentable_id=TEST_COMMENTABLE,
                        comment_thread_id=i,
                    )
                    for i in xrange(1, 6)
                ],
            }
        ]

        with patch('notifier.tasks.get_flagged_threads', return_value=test_threads), \
                patch('notifier.tasks.get_moderators', return_value=test_moderators) as m, \
                patch('notifier.tasks.generate_and_send_digests_flagged') as t:
            task_result = do_forums_digests_flagged.delay()
            self.assertTrue(task_result.successful())
            m.assert_called_with(TEST_COURSE_ID)
            self.assertEqual(t.delay.call_count, 2)
            t.delay.assert_called_with(last_message_batch)

    @override_settings(FORUM_DIGEST_TASK_BATCH_SIZE=10)
    def test_do_forums_digests_flagged_user_api_unavailable(self):
        """
        Test that retries are attempted if user API is down.
        """
        test_threads = make_flagged_threads()

        with patch('notifier.tasks.get_flagged_threads', return_value=test_threads), \
                patch('notifier.tasks.get_moderators', side_effect=UserServiceException("could not connect!")) as m, \
                patch('notifier.tasks.generate_and_send_digests_flagged') as t:
            try:
                do_forums_digests_flagged.delay()
            except UserServiceException as e:
                self.assertEqual(m.call_count, settings.DAILY_TASK_MAX_RETRIES + 1)
                self.assertEqual(t.call_count, 0)

    @override_settings(FORUM_DIGEST_TASK_BATCH_SIZE=10)
    def test_do_forums_digests_flagged_cs_api_unavailable(self):
        """
        Test that retries are attempted if comments service API is down.
        """
        with patch('notifier.tasks.get_flagged_threads', side_effect=CommentsServiceException("could not connect!")) as f, \
                patch('notifier.tasks.generate_and_send_digests_flagged') as t:
            try:
                do_forums_digests_flagged.delay()
            except CommentsServiceException as e:
                self.assertEqual(f.call_count, settings.DAILY_TASK_MAX_RETRIES + 1)
                self.assertEqual(t.call_count, 0)

    @override_settings(FORUM_DIGEST_TASK_BATCH_SIZE=10)
    def test_do_forums_digests(self):
        # patch _time_slice
        # patch get_digest_subscribers
        dt1 = datetime.datetime.utcnow()
        dt2 = dt1 + datetime.timedelta(days=1)
        with nested(
            patch('notifier.tasks.get_digest_subscribers', return_value=(usern(n) for n in xrange(11))),
            patch('notifier.tasks.generate_and_send_digests'),
            patch('notifier.tasks._time_slice', return_value=(dt1, dt2))
        ) as (p, t, ts):
            task_result = do_forums_digests.delay()
            self.assertTrue(task_result.successful())
            self.assertEqual(t.delay.call_count, 2)
            t.delay.assert_called_with([usern(10)], dt1, dt2)


    @override_settings(FORUM_DIGEST_TASK_BATCH_SIZE=10)
    def test_do_forums_digests_user_api_unavailable(self):
        # patch _time_slice
        # patch get_digest_subscribers
        dt1 = datetime.datetime.utcnow()
        dt2 = dt1 + datetime.timedelta(days=1)
        with nested(
            patch('notifier.tasks.get_digest_subscribers', side_effect=UserServiceException("could not connect!")),
            patch('notifier.tasks.generate_and_send_digests'),
        ) as (p, t):
            try:
                task_result = do_forums_digests.delay()
            except UserServiceException as e:
                self.assertEqual(p.call_count, settings.DAILY_TASK_MAX_RETRIES + 1)
                self.assertEqual(t.call_count, 0)
            else:
                # should have raised
                self.fail("task did not give up after exactly 3 attempts")

