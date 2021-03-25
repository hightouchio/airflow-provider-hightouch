import time
from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class HightouchHook(HttpHook):
    """
    Hook for Hightouch API

    :param hightouch_conn_id: Required. The name of the Airflow connection
        with connection information for the Hightouch API
    :param api_version: Opitional. Hightouch API version.
    :type api_version: str
    """

    PENDING = "pending"
    SUCCESS = "succeeded"
    RUNNING = "running"
    ERROR = "error"

    def __init__(
        self, hightouch_conn_id: str = "hightouch_default", api_version: str = "v1"
    ):
        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        super().__init__(http_conn_id=hightouch_conn_id)

    def poll(
        self, sync_id: int, wait_seconds: float = 3, timeout: Optional[int] = 3600
    ) -> None:
        """
        Polls the Hightouch API for sync status

        :param sync_id: Required. Id of the Hightouch sync to poll.
        :type sync_id: int
        :param wait_seconds: Optional. Number of seconds between checks.
        :type wait_seconds: float
        :param timeout: Optional. How many seconds to wait for job to complete.
        :type timeout: int
        """
        state = None
        start = time.monotonic()
        while True:
            if timeout and start + timeout < time.monotonic():
                raise AirflowException(
                    f"Timeout: Hightouch job {sync_id} was not ready after {timeout} seconds."
                )
            time.sleep(wait_seconds)
            try:
                job = self.get_sync_status(sync_id=sync_id)
                state = job.json()["sync"][
                    "status"
                ]  # TODO: Get state schema from Ernest

            except AirflowException as err:
                self.log.info(
                    "Retrying. Hightouch API returned a server error when waiting for job: %s",
                    err,
                )
                continue

            # TODO: Handle all errors
            if state in (self.RUNNING, self.PENDING):
                continue
            if state == self.SUCCESS:
                break
            if state == self.ERROR:
                raise AirflowException(f"Job {sync_id} failed to complete")
            else:
                raise AirflowException("Unhandled state: {state}")

    def submit_sync(self, sync_id: int) -> Any:
        """
        Submits a request to start a Sync
        """
        # Unfortunately the HTTP method is defined in the Hook as a class
        self.method = "POST"
        conn = self.get_connection(self.hightouch_conn_id)
        token = conn.password

        return self.run(
            endpoint=f"api/{self.api_version}/rest/triggerSync/{sync_id}",
            headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        )

    def get_sync_status(self, sync_id: int) -> Any:
        """
        Retrieves the current sync status
        """
        # Unfortunately the HTTP method is defined in the Hook as a class
        # attribute
        self.method = "GET"
        conn = self.get_connection(self.hightouch_conn_id)
        token = conn.password

        return self.run(
            endpoint=f"api/{self.api_version}/rest/syncStatus/{sync_id}",
            headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        )
