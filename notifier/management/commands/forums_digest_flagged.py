"""
Send a digest list of threads with flagged posts to forum moderators.
"""
from __future__ import print_function

from optparse import make_option

from django.conf import settings
from django.core.management.base import BaseCommand

from notifier.digest import render_digest_flagged
from notifier.pull import get_flagged_threads
from notifier.tasks import COMMENT_THREAD_URL_FORMAT
from notifier.tasks import do_forums_digests_flagged
from notifier.user import get_moderators


class Command(BaseCommand):
    """
    This Command is used to send a digest of threads with flagged posts to
    forum moderators.
    """

    help = "Send a digest list of threads with flagged posts to each moderator"
    option_list = BaseCommand.option_list + (
        make_option(
            '--is-dry-run',
            action='store_true',
            dest='is_dry_run',
            default=False,
            help='for each course with flagged posts, display the email address'
                 ' for each recipient, and the rendered email text WITHOUT'
                 ' sending a message',
        ),
    )

    def handle(self, *args, **options):
        """
        Handle a request to send a digest of threads with flagged posts to forum
        moderators.
        """
        is_dry_run = options.get('is_dry_run')

        if is_dry_run:
            dry_run()
        else:
            do_forums_digests_flagged()


def dry_run():
    """
    Print the recipients and rendered email text for each course with flagged posts.
    """
    course_threads = {}
    for thread in get_flagged_threads():
        course_id = thread.get('course_id')
        commentable_id = thread.get('commentable_id')
        comment_thread_id = thread.get('comment_thread_id')
        if course_id and commentable_id and comment_thread_id:
            if course_id not in course_threads:
                course_threads[course_id] = []
            thread_url = COMMENT_THREAD_URL_FORMAT.format(
                base_url=settings.LMS_URL_BASE,
                course_id=course_id,
                commentable_id=commentable_id,
                comment_thread_id=comment_thread_id,
            )
            course_threads[course_id].append(thread_url)

    for course_id in course_threads:
        moderators = get_moderators(course_id)
        try:
            # We only grab the first moderator because we're only concerned with rendering a single sample message.
            # THe assumption is that each message will be the same for all moderators in a course.
            user = moderators.next()
        except StopIteration:
            print('Course {course_id} has flagged posts but no moderators'.format(course_id=course_id))
            continue
        text, _ = render_digest_flagged({
            'course_id': course_id,
            'threads': course_threads[course_id],
            'recipient': user,
        })
        print('==============================')
        print("To: {email_address}".format(email_address=user.get('email')))
        for user in moderators:
            print("To: {email_address}".format(email_address=user.get('email')))
        print("Subject: {subject}".format(subject=settings.FORUM_DIGEST_EMAIL_SUBJECT_FLAGGED))
        print(text)
        print('==============================')
