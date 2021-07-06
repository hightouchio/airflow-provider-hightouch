"""
Unittest module to test Hightouch Operator.

Requires the unittest and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_hightouch_operator.TestHightouchOperator

"""

import unittest
from unittest import mock

import requests_mock

from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator


@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_HIGHTOUCH_DEFAULT="http://https%3A%2F%2Ftest.hightouch.io%2F",
)
class TestHightouchOperator(unittest.TestCase):
    @requests_mock.mock()
    def test_hightouch_operator(self, requests_mock):
        requests_mock.post(
            "https://test.hightouch.io/api/v2/rest/run/1",
            json={"success": "it worked"},
        )
        operator = HightouchTriggerSyncOperator(task_id="run", sync_id=1)

        response = operator.execute(context={})
        assert response == {"success": "it worked"}
