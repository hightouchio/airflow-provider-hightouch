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
        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1/runs",
            json={
                "data": [
                    {
                        "id": "42",
                        "startedAt": "2022-02-08T16:11:04.712Z",
                        "createdAt": "2022-02-08T16:11:04.712Z",
                        "finishedAt": "2022-02-08T16:11:11.698Z",
                        "querySize": 773,
                        "status": "success",
                        "completionRatio": 0.54,
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
                        "error": None,
                    },
                    {
                        "id": "43",
                        "startedAt": "2022-02-08T16:11:04.712Z",
                        "createdAt": "2022-02-08T17:44:05.198Z",
                        "finishedAt": "2022-02-08T17:44:25.366Z",
                        "querySize": 773,
                        "status": "success",
                        "completionRatio": 0.54,
                        "plannedRows": {
                            "addedCount": 0,
                            "changedCount": 765,
                            "removedCount": 0,
                        },
                        "successfulRows": {
                            "addedCount": 0,
                            "changedCount": 765,
                            "removedCount": 0,
                        },
                        "failedRows": {
                            "addedCount": 0,
                            "changedCount": 0,
                            "removedCount": 0,
                        },
                        "error": None,
                    },
                ]
            },
        )
        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1",
            json={
                "id": "1",
                "slug": "testsync",
                "workspaceId": "1",
                "createdAt": "2022-02-16T21:37:58.510Z",
                "updatedAt": "2022-02-16T21:37:58.510Z",
                "destinationId": "1",
                "modelId": "1",
                "configuration": {
                    "mode": "upsert",
                    "object": "contacts",
                    "mappings": [
                        {"to": "email", "from": "email", "type": "standard"},
                        {"to": "phone", "from": "phone", "type": "standard"},
                    ],
                    "objectId": "0-1",
                    "externalIdMapping": {
                        "to": "firstname",
                        "from": "test_id",
                        "type": "standard",
                    },
                    "associationMappings": [],
                },
                "schedule": {
                    "type": "interval",
                    "schedule": {"interval": {"unit": "day", "quantity": 1}},
                },
                "disabled": False,
                "status": "success",
                "lastRunAt": "2022-02-16T21:37:58.510Z",
                "referencedColumns": ["email", "name"],
                "primaryKey": "id",
            },
        )
        requests_mock.post(
            "https://test.hightouch.io/api/v1/syncs/trigger",
            json={"id": "123"},
        )
        operator = HightouchTriggerSyncOperator(task_id="run", sync_id=1)

        operator.execute(context={})
