"""
Unittest module to test Hightouch Hook.

Requires the unittest and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_hightouch_hook.TestHightouchHook

"""

import unittest
from unittest import mock

import requests_mock

from airflow_provider_hightouch.hooks.hightouch import HightouchHook


@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_HIGHTOUCH_DEFAULT="http://https%3A%2F%2Ftest.hightouch.io%2F",
)
class TestHightouchHook(unittest.TestCase):
    @requests_mock.mock()
    def test_hightouch_get_sync_status(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/1",
            json={"sync": {"sync_status": "success"}},
        )
        hook = HightouchHook(api_version="v2")
        response = hook.get_sync_status(1)
        assert response.json()["sync"]["sync_status"] == "success"

    @requests_mock.mock()
    def test_hightouch_submit_sync(self, requests_mock):
        requests_mock.post(
            "https://test.hightouch.io/api/v2/rest/run/1",
            json={"sync": {"sync_status": "success"}},
        )
        hook = HightouchHook()
        response = hook.submit_sync(1)
        assert response.json()["sync"]["sync_status"] == "success"
