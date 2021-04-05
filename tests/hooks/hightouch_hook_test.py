from unittest import mock

from airflow_provider_hightouch.hooks.hightouch import HightouchHook


@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_HIGHTOUCH_DEFAULT="http://https%3A%2F%2Ftest.hightouch.io%2F",
)
class TestHightouchHook:
    def test_hightouch_get_sync(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v1/rest/syncStatus/1", json={"success": True}
        )
        hook = HightouchHook()
        response = hook.get_sync_status(1)
        assert response.json()["success"]

    def test_hightouch_submit_sync(self, requests_mock):
        requests_mock.post(
            "https://test.hightouch.io/api/v1/rest/triggerSync/1",
            json={"success": True},
        )
        hook = HightouchHook()
        response = hook.submit_sync(1)
        assert response.json()["success"]
