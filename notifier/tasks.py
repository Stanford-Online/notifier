"""
Celery tasks for generating and sending digest emails.
"""
from contextlib import closing
from datetime import datetime, timedelta
import logging
import platform
import requests

from boto.ses.exceptions import SESMaxSendingRateExceededError
import celery
from django.conf import settings
from django.core.mail import EmailMultiAlternatives

from notifier.connection_wrapper import get_connection
from notifier.digest import render_digest
from notifier.digest import render_digest_flagged
from notifier.models import ForumDigestTask
from notifier.pull import generate_digest_content, CommentsServiceException
from notifier.pull import get_flagged_threads
from notifier.user import get_digest_subscribers, UserServiceException
from notifier.user import get_moderators

logger = logging.getLogger(__name__)

COMMENT_THREAD_URL_FORMAT = '/'.join([
    "{base_url}",
    "courses",
    "{course_id}",
    "discussion",
    "forum",
    "{commentable_id}",
    "threads",
    "{comment_thread_id}",
])

DEFAULT_LANGUAGE = 'en'


@celery.task(rate_limit=settings.FORUM_DIGEST_TASK_RATE_LIMIT, max_retries=settings.FORUM_DIGEST_TASK_MAX_RETRIES)
def generate_and_send_digests(users, from_dt, to_dt, language=None):
    """
    This task generates and sends forum digest emails to multiple users in a
    single background operation.

    `users` is an iterable of dictionaries, as returned by the edx user_api
    (required keys are "id", "name", "email", "preferences", and "course_info").

    `from_dt` and `to_dt` are datetime objects representing the start and end
    of the time window for which to generate a digest.
    """
    settings.LANGUAGE_CODE = language or settings.LANGUAGE_CODE or DEFAULT_LANGUAGE
    users_by_id = dict((str(u['id']), u) for u in users)
    msgs = []
    try:
        with closing(get_connection()) as cx:
            for user_id, digest in generate_digest_content(users_by_id, from_dt, to_dt):
                user = users_by_id[user_id]
                # format the digest
                text, html = render_digest(
                    user, digest, settings.FORUM_DIGEST_EMAIL_TITLE, settings.FORUM_DIGEST_EMAIL_DESCRIPTION)
                # send the message through our mailer
                msg = EmailMultiAlternatives(
                    settings.FORUM_DIGEST_EMAIL_SUBJECT,
                    text,
                    settings.FORUM_DIGEST_EMAIL_SENDER,
                    [user['email']]
                )
                msg.attach_alternative(html, "text/html")
                msgs.append(msg)
            if msgs:
                cx.send_messages(msgs)
            if settings.DEAD_MANS_SNITCH_URL:
                requests.post(settings.DEAD_MANS_SNITCH_URL)
    except (CommentsServiceException, SESMaxSendingRateExceededError) as e:
        # only retry if no messages were successfully sent yet.
        if not any((getattr(msg, 'extra_headers', {}).get('status') == 200 for msg in msgs)):
            raise generate_and_send_digests.retry(exc=e)
        else:
            # raise right away, since we don't support partial retry
            raise


@celery.task(rate_limit=settings.FORUM_DIGEST_TASK_RATE_LIMIT, max_retries=settings.FORUM_DIGEST_TASK_MAX_RETRIES)
def generate_and_send_digests_flagged(raw_msgs):
    """
    This task generates and sends flagged forum digest emails to multiple users
    in a single background operation.

    Args:
        raw_msgs (list): contains dicts with the following keys:
            course_id (str): identifier of the course
            recipient (dict): a single user dict
            threads (list): a list of thread URLs
    """
    rendered_msgs = []
    for raw_msg in raw_msgs:
        text, html = render_digest_flagged(raw_msg)
        rendered_msg = EmailMultiAlternatives(
            settings.FORUM_DIGEST_EMAIL_SUBJECT_FLAGGED,
            text,
            settings.FORUM_DIGEST_EMAIL_SENDER,
            [raw_msg['recipient']['email']],
        )
        rendered_msg.attach_alternative(html, "text/html")
        rendered_msgs.append(rendered_msg)
    with closing(get_connection()) as connection:
        try:
            connection.send_messages(rendered_msgs)
        except SESMaxSendingRateExceededError as error:
            # we've tripped the per-second send rate limit.  we generally
            # rely  on the django_ses auto throttle to prevent this,
            # but in case we creep over, we can re-queue and re-try this task
            # - if and only if none of the messages in our batch were
            # sent yet.
            if not any((
                    getattr(rendered_msg, 'extra_headers', {}).get('status') == 200
                    for rendered_msg in rendered_msgs
            )):
                raise generate_and_send_digests_flagged.retry(exc=error)
            else:
                # raise right away, since we don't support partial retry
                raise


def _time_slice(minutes, now=None):
    """
    Returns the most recently-elapsed time slice of the specified length (in
    minutes), as of the specified datetime (defaults to utcnow).
    
    `minutes` must be greater than one, less than or equal to 1440, and a factor 
    of 1440 (so that no time slice spans across multiple days). 
    
    >>> _time_slice(1, datetime(2013, 1, 1, 0, 0))
    (datetime.datetime(2012, 12, 31, 23, 59), datetime.datetime(2013, 1, 1, 0, 0))
    >>> _time_slice(1, datetime(2013, 1, 1, 0, 1))
    (datetime.datetime(2013, 1, 1, 0, 0), datetime.datetime(2013, 1, 1, 0, 1))
    >>> _time_slice(1, datetime(2013, 1, 1, 1, 1))
    (datetime.datetime(2013, 1, 1, 1, 0), datetime.datetime(2013, 1, 1, 1, 1))
    >>> _time_slice(15, datetime(2013, 1, 1, 0))
    (datetime.datetime(2012, 12, 31, 23, 45), datetime.datetime(2013, 1, 1, 0, 0))
    >>> _time_slice(15, datetime(2013, 1, 1, 0, 14))
    (datetime.datetime(2012, 12, 31, 23, 45), datetime.datetime(2013, 1, 1, 0, 0))
    >>> _time_slice(15, datetime(2013, 1, 1, 0, 14, 59))
    (datetime.datetime(2012, 12, 31, 23, 45), datetime.datetime(2013, 1, 1, 0, 0))
    >>> _time_slice(15, datetime(2013, 1, 1, 0, 15, 0))
    (datetime.datetime(2013, 1, 1, 0, 0), datetime.datetime(2013, 1, 1, 0, 15))
    >>> _time_slice(1440, datetime(2013, 1, 1))
    (datetime.datetime(2012, 12, 31, 0, 0), datetime.datetime(2013, 1, 1, 0, 0))
    >>> _time_slice(1440, datetime(2013, 1, 1, 23, 59))
    (datetime.datetime(2012, 12, 31, 0, 0), datetime.datetime(2013, 1, 1, 0, 0))
    >>> e = None
    >>> try:
    ...     _time_slice(14, datetime(2013, 1, 2, 0, 0))
    ... except AssertionError, e:
    ...     pass
    ... 
    >>> e is not None
    True
    """
    assert minutes > 0
    assert minutes <= 1440
    assert 1440 % minutes == 0
    now = now or datetime.utcnow()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) 
    minutes_since_midnight = (now - midnight).seconds / 60
    dt_end = midnight + timedelta(minutes=(minutes_since_midnight / minutes) * minutes)
    dt_start = dt_end - timedelta(minutes=minutes)
    return (dt_start, dt_end)


@celery.task(max_retries=settings.DAILY_TASK_MAX_RETRIES, default_retry_delay=settings.DAILY_TASK_RETRY_DELAY)
def do_forums_digests_flagged():
    """
    Generate and batch send digest emails for each thread with flagged posts.
    """
    def get_course_threads():
        """
        Process threads from comments service API and group by course_id.

        Returns:
            dict: key=course_id, value=list of thread URLs with flagged posts
        """
        output = {}
        for thread in get_flagged_threads():
            course_id = thread.get('course_id')
            commentable_id = thread.get('commentable_id')
            comment_thread_id = thread.get('comment_thread_id')
            if course_id and commentable_id and comment_thread_id:
                if course_id not in output:
                    output[course_id] = []
                thread_url = COMMENT_THREAD_URL_FORMAT.format(
                    base_url=settings.LMS_URL_BASE,
                    course_id=course_id,
                    commentable_id=commentable_id,
                    comment_thread_id=comment_thread_id,
                )
                output[course_id].append(thread_url)
        return output

    def get_messages():
        """
        Yield message dicts to be used for sending digest emails to moderators.

        Returns:
            dict generator:
                course_id (str): course identifier
                threads (list): strings of thread URLs
                recipient (dict): a single user
        """
        course_threads = get_course_threads()
        for course_id in course_threads:
            for user in get_moderators(course_id):
                yield {
                    'course_id': course_id,
                    'threads': course_threads[course_id],
                    'recipient': user,
                }

    def batch_messages():
        """
        Yield batches of messages to send.
        """
        batch = []
        for message in get_messages():
            batch.append(message)
            if len(batch) == settings.FORUM_DIGEST_TASK_BATCH_SIZE:
                yield batch
                batch = []
        if batch:
            yield batch

    try:
        for batch in batch_messages():
            generate_and_send_digests_flagged.delay(batch)
    except (CommentsServiceException, UserServiceException) as error:
        raise do_forums_digests_flagged.retry(exc=error)


@celery.task(
    bind=True,
    max_retries=settings.DAILY_TASK_MAX_RETRIES,
    default_retry_delay=settings.DAILY_TASK_RETRY_DELAY
)
def do_forums_digests(self):

    def batch_digest_subscribers():
        batch = []
        for v in get_digest_subscribers():
            batch.append(v)
            if len(batch)==settings.FORUM_DIGEST_TASK_BATCH_SIZE:
                yield batch
                batch = []
        if batch:
            yield batch

    from_dt, to_dt = _time_slice(settings.FORUM_DIGEST_TASK_INTERVAL)

    # Remove old tasks from the database so that the table doesn't keep growing forever.
    ForumDigestTask.prune_old_tasks(settings.FORUM_DIGEST_TASK_GC_DAYS)

    if self.request.retries == 0:
        task, created = ForumDigestTask.objects.get_or_create(
            from_dt=from_dt,
            to_dt=to_dt,
            defaults={'node': platform.node()}
        )
        if created:
            logger.info("Beginning forums digest task: from_dt=%s to_dt=%s", from_dt, to_dt)
        else:
            logger.info(
                "Forums digest task already scheduled by '%s'; skipping: from_dt=%s to_dt=%s",
                task.node, from_dt, to_dt
            )
            return
    else:
        logger.info("Retrying forums digest task: from_dt=%s to_dt=%s", from_dt, to_dt)

    try:
        for user_batch in batch_digest_subscribers():
            generate_and_send_digests.delay(user_batch, from_dt, to_dt, language=settings.LANGUAGE_CODE)
    except UserServiceException, e:
        raise do_forums_digests.retry(exc=e)
