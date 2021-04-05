from unittest import mock

from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator


@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_HIGHTOUCH_DEFAULT="http://https%3A%2F%2Ftest.hightouch.io%2F",
)
class TestHightouchHook:
    def test_hightouch_operator(self, requests_mock):
        requests_mock.post(
            "https://test.hightouch.io/api/v1/rest/triggerSync/1",
            json={"success": "it worked"},
        )
        operator = HightouchTriggerSyncOperator(task_id="run", sync_id=1)

        response = operator.execute(context={})
        assert response == {"success": "it worked"}
