"""
Unittest module to test Hightouch Hook.

Requires the unittest and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_hightouch_hook.TestHightouchHook

"""

import unittest
from unittest import mock

import pytest
import requests_mock
from airflow import AirflowException

from airflow_provider_hightouch.hooks.hightouch import HightouchHook


def payload_factory(status, last_sync_run=True):
    last_sync_run_payload = {}
    if last_sync_run:
        last_sync_run_payload = {
            "sync_run_id": 20286808,
            "sync_run_created_at": "2022-01-27T23:19:12.868804+00:00",
            "sync_run_finished_at": None,
            "sync_run_error": None,
        }
    return {"sync": {"sync_status": status}, "last_sync_run": last_sync_run_payload}


@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_HIGHTOUCH_DEFAULT="http://https%3A%2F%2Ftest.hightouch.io%2F",
)
class TestHightouchHook(unittest.TestCase):
    @requests_mock.mock()
    def test_hightouch_get_sync_status(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/1",
            json=payload_factory("success"),
        )
        hook = HightouchHook(api_version="v2")
        response = hook.get_sync_status(1)
        assert response.json()["sync"]["sync_status"] == "success"

    @requests_mock.mock()
    def test_hightouch_submit_sync(self, requests_mock):
        requests_mock.post(
            "https://test.hightouch.io/api/v2/rest/run/1",
            json=payload_factory("success"),
        )
        hook = HightouchHook()
        response = hook.submit_sync(1)
        assert response.json()["sync"]["sync_status"] == "success"

    @requests_mock.mock()
    def test_get_status_with_sync_run_id(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/1?latest_sync_run_id=123",
            json=payload_factory("success"),
        )
        hook = HightouchHook()
        response = hook.get_sync_status(1, latest_sync_run_id=123)
        assert response.json()["sync"]["sync_status"] == "success"

    @requests_mock.mock()
    def test_hightouch_hook_failed(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/23064",
            json=payload_factory("failed"),
        )
        hook = HightouchHook()
        response = hook.get_sync_status(23064)
        assert response.json()["sync"]["sync_status"] == "failed"

    @requests_mock.mock()
    def test_poll_success(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/1",
            json=payload_factory("success"),
        )
        hook = HightouchHook()
        hook.poll(sync_id=1, wait_seconds=1, timeout=5, error_on_warning=True)

    @requests_mock.mock()
    def test_poll_fail_dont_ignore_warning(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/1",
            json=payload_factory("warning"),
        )
        hook = HightouchHook()
        with pytest.raises(AirflowException) as excinfo:
            hook.poll(sync_id=1, wait_seconds=1, timeout=5, error_on_warning=True)
            assert "Job 1 failed to complete with status warning" in str(excinfo.value)

    @requests_mock.mock()
    def test_poll_success_ignore_warning(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/1",
            json=payload_factory("warning"),
        )
        hook = HightouchHook()
        hook.poll(sync_id=1, wait_seconds=1, timeout=5, error_on_warning=False)

    @requests_mock.mock()
    def test_poll_failed(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v2/rest/sync/1",
            json=payload_factory("failed"),
        )
        hook = HightouchHook()
        with pytest.raises(AirflowException) as excinfo:
            hook.poll(sync_id=1, wait_seconds=0.11, timeout=5, error_on_warning=True)
            assert "Job 1 failed to complete with status failed" in str(excinfo.value)

    @requests_mock.mock()
    def test_poll_pending_then_complete(self, requests_mock):
        requests_mock.register_uri(
            "GET",
            "https://test.hightouch.io/api/v2/rest/sync/1",
            [
                {"json": payload_factory("pending")},
                {"json": payload_factory("success")},
            ],
        )
        hook = HightouchHook()
        hook.poll(sync_id=1, wait_seconds=0.1, timeout=2, error_on_warning=True)

    @requests_mock.mock()
    def test_poll_pending_timeout(self, requests_mock):
        requests_mock.register_uri(
            "GET",
            "https://test.hightouch.io/api/v2/rest/sync/1",
            [
                {"json": payload_factory("pending")},
            ],
        )
        hook = HightouchHook()
        with pytest.raises(AirflowException) as excinfo:
            hook.poll(sync_id=1, wait_seconds=0.5, timeout=1, error_on_warning=True)
            assert "Timeout: Hightouch job 1 was not ready after 1 seconds." in str(
                excinfo.value
            )

    @requests_mock.mock()
    def test_poll_no_last_sync_run_retries(self, requests_mock):

        requests_mock.register_uri(
            "GET",
            "https://test.hightouch.io/api/v2/rest/sync/1",
            [
                {"json": payload_factory("pending", False)},
                {"json": payload_factory("warning", False)},
                {"json": payload_factory("pending", True)},
                {"json": payload_factory("success", True)},
            ],
        )
        hook = HightouchHook()
        hook.poll(sync_id=1, wait_seconds=0.2, timeout=5, error_on_warning=True)
        assert requests_mock.call_count == 4
