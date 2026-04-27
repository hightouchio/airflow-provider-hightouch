"""
Unittest module to test Hightouch Sensor.

Requires the unittest and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.sensors.test_hightouch_sensor.TestHightouchSensor

"""

import unittest
from unittest import mock

import requests_mock
from airflow.exceptions import AirflowException

from airflow_provider_hightouch.sensors.hightouch import HightouchSyncRunSensor


def sync_run_payload(status="success", error=None):
    return {
        "data": [
            {
                "id": "42",
                "startedAt": "2022-02-08T16:11:04.712Z",
                "createdAt": "2022-02-08T16:11:04.712Z",
                "finishedAt": "2022-02-08T16:11:11.698Z",
                "querySize": 773,
                "status": status,
                "completionRatio": 1.0,
                "plannedRows": {
                    "addedCount": 773,
                    "changedCount": 0,
                    "removedCount": 0,
                },
                "successfulRows": {
                    "addedCount": 773,
                    "changedCount": 0,
                    "removedCount": 0,
                },
                "failedRows": {
                    "addedCount": 0,
                    "changedCount": 0,
                    "removedCount": 0,
                },
                "error": error,
            }
        ]
    }


@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_HIGHTOUCH_DEFAULT='{ "conn_type": "https", "host": "test.hightouch.io", "schema": "https"}',
)
class TestHightouchSensor(unittest.TestCase):
    @requests_mock.mock()
    def test_sensor_poke_success(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1/runs",
            json=sync_run_payload(status="success"),
        )
        sensor = HightouchSyncRunSensor(
            task_id="test_sensor",
            sync_run_id="42",
            sync_id="1",
        )
        result = sensor.poke(context={})
        assert result is True

    @requests_mock.mock()
    def test_sensor_poke_pending(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1/runs",
            json=sync_run_payload(status="processing"),
        )
        sensor = HightouchSyncRunSensor(
            task_id="test_sensor",
            sync_run_id="42",
            sync_id="1",
        )
        result = sensor.poke(context={})
        assert result is False

    @requests_mock.mock()
    def test_sensor_poke_failure(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1/runs",
            json=sync_run_payload(status="failed", error="Something went wrong"),
        )
        sensor = HightouchSyncRunSensor(
            task_id="test_sensor",
            sync_run_id="42",
            sync_id="1",
        )
        with self.assertRaises(AirflowException):
            sensor.poke(context={})

    @requests_mock.mock()
    def test_sensor_poke_warning_no_error(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1/runs",
            json=sync_run_payload(status="warning"),
        )
        sensor = HightouchSyncRunSensor(
            task_id="test_sensor",
            sync_run_id="42",
            sync_id="1",
            error_on_warning=False,
        )
        result = sensor.poke(context={})
        assert result is True

    @requests_mock.mock()
    def test_sensor_poke_warning_with_error(self, requests_mock):
        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1/runs",
            json=sync_run_payload(status="warning", error="Warning occurred"),
        )
        sensor = HightouchSyncRunSensor(
            task_id="test_sensor",
            sync_run_id="42",
            sync_id="1",
            error_on_warning=True,
        )
        with self.assertRaises(AirflowException):
            sensor.poke(context={})
