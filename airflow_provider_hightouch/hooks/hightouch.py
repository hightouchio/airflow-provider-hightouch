import time
from typing import Any, Optional

from airflow.exceptions import AirflowException

try:
    from airflow.providers.http.hooks.http import HttpHook
except ImportError:
    from airflow.hooks.http_hook import HttpHook

from airflow_provider_hightouch import __version__


class HightouchHook(HttpHook):
    """
    Hook for Hightouch API

    :param hightouch_conn_id: Required. The name of the Airflow connection
        with connection information for the Hightouch API
    :param api_version: Opitional. Hightouch API version.
    :type api_version: str
    """

    CANCELLED = "cancelled"
    FAILED = "failed"
    PENDING = "pending"
    WARNING = "warning"
    SUCCESS = "success"
    QUERYING = "querying"
    PROCESSING = "processing"
    ABORTED = "aborted"
    QUEUED = "queued"

    def __init__(
        self, hightouch_conn_id: str = "hightouch_default", api_version: str = "v2"
    ):
        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        if api_version != "v2":
            raise AirflowException("Only v2 of the API is currently supported")
        super().__init__(http_conn_id=hightouch_conn_id)

    def poll(
        self,
        sync_id: int,
        wait_seconds: float = 3,
        timeout: int = 3600,
        latest_sync_run_id: Optional[int] = None,
        error_on_warning: bool = False,
    ) -> None:
        """
        Polls the Hightouch API for sync status

        :param sync_id: Required. Id of the Hightouch sync to poll.
        :type sync_id: int
        :param wait_seconds: Optional. Number of seconds between checks.
        :type wait_seconds: float
        :param timeout: Optional. How many seconds to wait for job to complete.
        :type timeout: int
        :param latest_sync_run_id:: Optional. When provided, returns the status
            for the sync run immediately following the sync_run_id provided.
            Otherwise, return the status for the latest run.
        :type latest_sync_run_id: int
        :param error_on_warning: Whether Hightouch warnings should be considered
            failures in the Airflow dag
        :type error_on_warning: bool
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
                job = self.get_sync_status(
                    sync_id=sync_id, latest_sync_run_id=latest_sync_run_id
                )
                state = job.json()["sync"]["sync_status"]
                last_sync_run = job.json().get("last_sync_run", {})

            except KeyError:
                self.log.warning("No status available for the provided sync run.")
                break

            except AirflowException as err:
                self.log.info(
                    "Retrying. Hightouch API returned a server error when waiting for job: %s",
                    err,
                )
                continue

            if not last_sync_run:
                self.log.info(
                    "Last Sync Run is empty. A sync run was likely in progress already."
                    + "Waiting for the previous run to complete."
                )
                continue

            if state in (self.CANCELLED, self.FAILED):
                raise AirflowException(
                    f"Job {sync_id} failed to complete with status {state}"
                )

            if state in (self.PENDING, self.QUERYING, self.PROCESSING, self.QUEUED):
                continue

            if state == self.WARNING:
                self.log.warning(f"Job {sync_id} completed but with warnings")
                if error_on_warning:
                    raise AirflowException(
                        f"Job {sync_id} failed to complete with status {state}"
                    )
                break

            if state == self.SUCCESS:
                break

            raise AirflowException("Unhandled state: {state}")

    def submit_sync(
        self,
        sync_id: int,
    ) -> Any:
        """
        Submits a request to start a Sync
        """
        # Unfortunately the HTTP method is defined in the Hook as a class attr
        self.method = "POST"
        conn = self.get_connection(self.hightouch_conn_id)
        token = conn.password

        endpoint = f"api/{self.api_version}/rest/run/{sync_id}"
        user_agent = f"AirflowHightouchOperator/" + __version__
        return self.run(
            endpoint=endpoint,
            headers={
                "accept": "application/json",
                "Authorization": f"Bearer {token}",
                "User-Agent": user_agent,
            },
        )

    def get_sync_status(
        self, sync_id: int, latest_sync_run_id: Optional[int] = None
    ) -> Any:
        """
        Retrieves the current sync status
        """
        # Unfortunately the HTTP method is defined in the Hook as a class attr
        self.method = "GET"
        conn = self.get_connection(self.hightouch_conn_id)
        token = conn.password

        endpoint = f"api/{self.api_version}/rest/sync/{sync_id}"
        if latest_sync_run_id:
            endpoint += "?latest_sync_run_id=" + str(latest_sync_run_id)
        response = self.run(
            endpoint=endpoint,
            headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        )
        self.log.info("Got response: %s", response.json())
        return response
