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


def sync_details_payload():
    return {
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
    }


@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_HIGHTOUCH_DEFAULT='{ "conn_type": "https", "host": "test.hightouch.io", "schema": "https"}',
)
class TestHightouchHook(unittest.TestCase):
    @requests_mock.mock()
    def test_hightouch_get_sync_status(self, requests_mock):

        requests_mock.get(
            "https://test.hightouch.io/api/v1/syncs/1",
            json=sync_details_payload(),
        )
        hook = HightouchHook(api_version="v3")
        response = hook.get_sync_details(1)
        assert response["status"] == "success"

    @requests_mock.mock()
    def test_hightouch_submit_sync_with_id(self, requests_mock):
        requests_mock.post(
            "https://test.hightouch.io/api/v1/syncs/trigger", json={"id": "123"}
        )
        hook = HightouchHook()
        response = hook.start_sync(sync_id=100)
        assert response == "123"

    @requests_mock.mock()
    def test_hightouch_submit_sync_with_slug(self, requests_mock):
        requests_mock.post(
            "https://test.hightouch.io/api/v1/syncs/trigger", json={"id": "123"}
        )
        hook = HightouchHook()
        response = hook.start_sync(sync_slug="boo")
        assert response == "123"
