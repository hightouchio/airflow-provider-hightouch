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

    def __init__(
        self,
        hightouch_conn_id: str = "hightouch_default",
        api_version: Optional[str] = "v1",
    ):
        super().__init__(http_conn_id=hightouch_conn_id)
        self.api_version = api_version

    def poll(
        self, sync_id: int, wait_seconds: float = 3, timeout: Optional[int] = 3600
    ) -> None:
        """
        Polls the Hightouch API for sync status

        :param sync_id: Required. Id of the Hightouch sync.
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
                job = self.get_sync(sync_id=sync_id)
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
            if state == "RUNNING":
                continue
            if state == "SUCCESS":
                break
            if state == "ERROR":
                raise AirflowException(f"Job {sync_id} failed to complete")
            else:
                raise AirflowException("Unhandled state: {state}")

    def submit_sync(self, sync_id: int) -> Any:
        """
        Submits a request to start a Sync

        TODO: What does the payload look like here?

        """
        return self.run(
            endpoint=f"api/{self.api_version}/sync",
            json={"id": sync_id},
            headers={"accept": "application/json"},
            method="POST",
        )

    def get_sync(self, sync_id: int) -> Any:
        """
        Retrieves the current sync status

        TODO: What does the payload look like here?
        """
        return self.run(
            endpoint=f"api/{self.api_version}/sync/{sync_id}",
            headers={"accept": "application/json"},
        )
