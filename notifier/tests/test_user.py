"""
"""
from django.test import TestCase
from django.test.utils import override_settings
from mock import patch

from notifier.user import get_digest_subscribers, DIGEST_NOTIFICATION_PREFERENCE_KEY
from notifier.user import get_moderators

from .utils import make_mock_json_response

TEST_API_KEY = 'ZXY123!@#$%'


# some shorthand to quickly generate fixture results
mkresult = lambda n: {
    "id": n,
    "email": "email%d" % n, 
    "name": "name%d" % n, 
    "preferences": {
        DIGEST_NOTIFICATION_PREFERENCE_KEY: "pref%d" % n,
    },
    "course_info": {},
}
mkexpected = lambda d: dict([(key, val) for (key, val) in d.items() if key != "url"])


def make_response(count=0):
    """Return fixture json response of users."""
    return {
        "count": count,
        "next": None,
        "previous": None,
        "results": [mkresult(i) for i in xrange(count)],
    }


@override_settings(US_API_KEY=TEST_API_KEY)
class RoleTestCase(TestCase):
    """
    Test forum roles for moderators
    """
    def setUp(self):
        """
        Setup common test state
        """
        self.course_id = "org/course/run"
        self.expected_api_url = "test_server_url/user_api/v1/forum_roles/Moderator/users/"
        self.expected_headers = {'X-EDX-API-Key': TEST_API_KEY}
        self.expected_params = {
            "page_size": 3,
            "page": 1,
            "course_id": self.course_id,
        }

    @override_settings(US_URL_BASE="test_server_url", US_RESULT_PAGE_SIZE=3)
    def test_get_moderators_empty(self):
        """
        Test that an empty moderator list can be retrieved
        """
        expected_empty = make_response()
        mock_response = make_mock_json_response(json=expected_empty)
        with patch('requests.get', return_value=mock_response) as patched:
            result = list(get_moderators(self.course_id))
            patched.assert_called_once_with(
                self.expected_api_url,
                params=self.expected_params,
                headers=self.expected_headers,
            )
            self.assertEqual(0, len(result))

    @override_settings(US_URL_BASE="test_server_url", US_RESULT_PAGE_SIZE=3)
    def test_get_moderators_single_page(self):
        """
        Test that a moderator list can be retrieved
        """
        expected = make_response(3)
        mock_response = make_mock_json_response(json=expected)
        with patch('requests.get', return_value=mock_response) as patched:
            result = get_moderators(self.course_id)
            result = list(result)
            patched.assert_called_once_with(
                self.expected_api_url,
                params=self.expected_params,
                headers=self.expected_headers
            )
            self.assertEqual(result, expected['results'])
            self.assertEqual(expected['count'], len(result))

    @override_settings(
        US_URL_BASE="test_server_url",
        US_RESULT_PAGE_SIZE=3,
        US_HTTP_AUTH_USER='someuser',
        US_HTTP_AUTH_PASS='somepass',
    )
    def test_get_moderators_basic_auth(self):
        """
        Test that basic auth works
        """
        expected = make_response(3)
        mock_response = make_mock_json_response(json=expected)
        with patch('requests.get', return_value=mock_response) as patched:
            result = get_moderators(self.course_id)
            result = list(result)
            patched.assert_called_once_with(
                self.expected_api_url,
                params=self.expected_params,
                headers=self.expected_headers,
                auth=('someuser', 'somepass'),
            )
            self.assertEqual(result, expected['results'])

    @override_settings(US_URL_BASE="test_server_url", US_RESULT_PAGE_SIZE=3)
    def test_get_moderators_multi_page(self):
        """
        Test that a moderator list can be paged
        """
        expected_pages = [
            {
                "count": 5,
                "next": "not none",
                "previous": None,
                "results": [
                    mkresult(i) for i in xrange(1, 4)
                ],
            },
            {
                "count": 5,
                "next": None,
                "previous": "not none",
                "results": [
                    mkresult(i) for i in xrange(4, 6)
                ],
            },
        ]

        mock_response = make_mock_json_response(json=expected_pages[0])
        with patch('requests.get', return_value=mock_response) as patched:
            result = []
            users = get_moderators(self.course_id)
            result.append(users.next())
            patched.assert_called_once_with(
                self.expected_api_url,
                params=self.expected_params,
                headers=self.expected_headers)
            result.append(users.next())
            result.append(users.next())  # result 3, end of page
            self.assertEqual(
                [
                    mkexpected(mkresult(i)) for i in xrange(1, 4)
                ],
                result
            )
            # still should only have called requests.get() once
            self.assertEqual(1, patched.call_count)

            patched.reset_mock()  # reset call count
            self.expected_params['page'] = 2
            mock_response.json.return_value = expected_pages[1]
            self.assertEqual(mkexpected(mkresult(4)), users.next())
            patched.assert_called_once_with(
                self.expected_api_url,
                params=self.expected_params,
                headers=self.expected_headers)
            self.assertEqual(mkexpected(mkresult(5)), users.next())
            self.assertEqual(1, patched.call_count)
            self.assertRaises(StopIteration, users.next)


@override_settings(US_API_KEY=TEST_API_KEY)
class UserTestCase(TestCase):
    """
    """

    def setUp(self):
        self.expected_api_url = "test_server_url/notifier_api/v1/users/"
        self.expected_params = {"page_size":3, "page":1}
        self.expected_headers = {'X-EDX-API-Key': TEST_API_KEY}


    @override_settings(US_URL_BASE="test_server_url", US_RESULT_PAGE_SIZE=3)
    def test_get_digest_subscribers_empty(self):
        """
        """

        # empty result
        mock_response = make_mock_json_response(json={
            "count": 0,
            "next": None,
            "previous": None,
            "results": []
        })

        with patch('requests.get', return_value=mock_response) as p:
            res = list(get_digest_subscribers())
            p.assert_called_once_with(
                    self.expected_api_url,
                    params=self.expected_params,
                    headers=self.expected_headers)
            self.assertEqual(0, len(res))


    @override_settings(US_URL_BASE="test_server_url", US_RESULT_PAGE_SIZE=3)
    def test_get_digest_subscribers_single_page(self):
        """
        """

        # single page result
        mock_response = make_mock_json_response(json={
            "count": 3,
            "next": None,
            "previous": None,
            "results": [mkresult(1), mkresult(2), mkresult(3)]
        })

        with patch('requests.get', return_value=mock_response) as p:
            res = list(get_digest_subscribers())
            p.assert_called_once_with(
                    self.expected_api_url,
                    params=self.expected_params,
                    headers=self.expected_headers)
            self.assertEqual([
                mkexpected(mkresult(1)), 
                mkexpected(mkresult(2)), 
                mkexpected(mkresult(3))], res)


    @override_settings(US_URL_BASE="test_server_url", US_RESULT_PAGE_SIZE=3)
    def test_get_digest_subscribers_multi_page(self):
        """
        """

        # multi page result
        expected_multi_p1 = {
            "count": 5,
            "next": "not none",
            "previous": None,
            "results": [mkresult(1), mkresult(2), mkresult(3)]
        }
        expected_multi_p2 = {
            "count": 5,
            "next": None,
            "previous": "not none",
            "results": [mkresult(4), mkresult(5)]
        }

        expected_pages = [expected_multi_p1, expected_multi_p2]
        def side_effect(*a, **kw):
            return expected_pages.pop(0)

        mock_response = make_mock_json_response(json=expected_multi_p1)
        with patch('requests.get', return_value=mock_response) as p:
            res = []
            g = get_digest_subscribers()
            res.append(g.next())
            p.assert_called_once_with(
                self.expected_api_url,
                params=self.expected_params,
                headers=self.expected_headers)
            res.append(g.next())
            res.append(g.next()) # result 3, end of page
            self.assertEqual([
                mkexpected(mkresult(1)),
                mkexpected(mkresult(2)),
                mkexpected(mkresult(3))], res)
            # still should only have called requests.get() once
            self.assertEqual(1, p.call_count)

        mock_response = make_mock_json_response(json=expected_multi_p2)
        with patch('requests.get', return_value=mock_response) as p:
            self.expected_params['page']=2
            self.assertEqual(mkexpected(mkresult(4)), g.next())
            p.assert_called_once_with(
                self.expected_api_url,
                params=self.expected_params,
                headers=self.expected_headers)
            self.assertEqual(mkexpected(mkresult(5)), g.next())
            self.assertEqual(1, p.call_count)
            self.assertRaises(StopIteration, g.next)


    @override_settings(US_URL_BASE="test_server_url", US_RESULT_PAGE_SIZE=3, US_HTTP_AUTH_USER='someuser', US_HTTP_AUTH_PASS='somepass')
    def test_get_digest_subscribers_basic_auth(self):
        """
        """
        # single page result
        mock_response = make_mock_json_response(json={
            "count": 3,
            "next": None,
            "previous": None,
            "results": [mkresult(1), mkresult(2), mkresult(3)]
        })

        with patch('requests.get', return_value=mock_response) as p:
            res = list(get_digest_subscribers())
            p.assert_called_once_with(
                    self.expected_api_url,
                    params=self.expected_params,
                    headers=self.expected_headers,
                    auth=('someuser', 'somepass'))
            self.assertEqual([
                mkexpected(mkresult(1)), 
                mkexpected(mkresult(2)), 
                mkexpected(mkresult(3))], res)



