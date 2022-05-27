import datetime
import json
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from airflow.exceptions import AirflowException

from airflow_provider_hightouch.consts import (
    DEFAULT_POLL_INTERVAL,
    HIGHTOUCH_API_BASE_V3,
    PENDING_STATUSES,
    SUCCESS,
    TERMINAL_STATUSES,
    WARNING,
)
from airflow_provider_hightouch.types import HightouchOutput

try:
    from airflow.providers.http.hooks.http import HttpHook
except ImportError:
    from airflow.hooks.http_hook import HttpHook

from airflow_provider_hightouch import __version__, utils


class HightouchHook(HttpHook):
    """
    Hook for Hightouch API

    Args:
        hightouch_conn_id (str):  The name of the Airflow connection
        with connection information for the Hightouch API
        api_version: (optional(str)). Hightouch API version.
    """

    def __init__(
        self,
        hightouch_conn_id: str = "hightouch_default",
        api_version: str = "v3",
        request_max_retries: int = 3,
        request_retry_delay: float = 0.5,
    ):
        self.hightouch_conn_id = hightouch_conn_id
        self.api_version = api_version
        self._request_max_retries = request_max_retries
        self._request_retry_delay = request_retry_delay
        if self.api_version not in ("v1", "v3"):
            raise AirflowException(
                "This version of the Hightouch Operator only supports the v1/v3 API."
            )
        super().__init__(http_conn_id=hightouch_conn_id)

    @property
    def api_base_url(self) -> str:
        """Returns the correct API BASE URL depending on the API version."""
        return HIGHTOUCH_API_BASE_V3

    def make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ):
        """Creates and sends a request to the desired Hightouch API endpoint
        Args:
            method (str): The http method use for this request (e.g. "GET", "POST").
            endpoint (str): The Hightouch API endpoint to send this request to.
            params (Optional(dict): Query parameters to pass to the API endpoint
            body (Optional(dict): Body parameters to pass to the API endpoint
        Returns:
            Dict[str, Any]: Parsed json data from the response to this request
        """

        conn = self.get_connection(self.hightouch_conn_id)
        token = conn.password

        user_agent = "AirflowHightouchOperator/" + __version__
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": user_agent,
        }

        num_retries = 0
        while True:
            try:
                self.method = method
                response = self.run(
                    endpoint=urljoin(self.api_base_url, endpoint),
                    data=data,
                    headers=headers,
                )
                resp_dict = response.json()
                return resp_dict["data"] if "data" in resp_dict else resp_dict
            except AirflowException as e:
                self.log.error("Request to Hightouch API failed: %s", e)
                if num_retries == self._request_max_retries:
                    break
                num_retries += 1
                time.sleep(self._request_retry_delay)

        raise AirflowException("Exceeded max number of retries.")

    def get_sync_run_details(
        self, sync_id: str, sync_request_id: str
    ) -> List[Dict[str, Any]]:
        """Get details about a given sync run from the Hightouch API.
        Args:
            sync_id (str): The Hightouch Sync ID.
            sync_request_id (str): The Hightouch Sync Request ID.
        Returns:
            Dict[str, Any]: Parsed json data from the response
        """
        params = {"runId": sync_request_id}
        return self.make_request(
            method="GET", endpoint=f"syncs/{sync_id}/runs", data=params
        )

    def get_sync_details(self, sync_id: str) -> Dict[str, Any]:
        """Get details about a given sync from the Hightouch API.
        Args:
            sync_id (str): The Hightouch Sync ID.
        Returns:
            Dict[str, Any]: Parsed json data from the response
        """
        return self.make_request(method="GET", endpoint=f"syncs/{sync_id}")

    def get_sync_from_slug(self, sync_slug: str) -> str:
        """Get details about a given sync from the Hightouch API.
        Args:
            sync_id (str): The Hightouch Sync ID.
        Returns:
            Dict[str, Any]: Parsed json data from the response
        """
        return self.make_request(
            method="GET", endpoint="syncs", data={"slug": sync_slug}
        )[0]["id"]

    def start_sync(
        self, sync_id: Optional[str] = None, sync_slug: Optional[str] = None
    ) -> str:
        """Trigger a sync and initiate a sync run
        Args:
            sync_id (str): The Hightouch Sync ID.
        Returns:
            str: The sync request ID created by the Hightouch API.
        """
        if sync_id:
            return self.make_request(
                method="POST", endpoint="syncs/trigger", data={"syncId": sync_id}
            )["id"]
        if sync_slug:
            return self.make_request(
                method="POST", endpoint="syncs/trigger", data={"syncSlug": sync_slug}
            )["id"]
        raise AirflowException(
            "One of sync_id or sync_slug must be provided to trigger a sync."
        )

    def poll_sync(
        self,
        sync_id: str,
        sync_request_id: str,
        fail_on_warning: bool = False,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = None,
    ) -> HightouchOutput:
        """Poll for the completion of a sync
        Args:
            sync_id (str): The Hightouch Sync ID
            sync_request_id (str): The Hightouch Sync Request ID to poll against.
            fail_on_warning (bool): Whether a warning is considered a failure for this sync.
            poll_interval (float): The time in seconds that will be waited between succcessive polls
            poll_timeout (float): The maximum time that will be waited before this operation
                times out.
        Returns:
            Dict[str, Any]: Parsed json output from the API
        """
        poll_start = datetime.datetime.now()
        while True:
            sync_run_details = self.get_sync_run_details(sync_id, sync_request_id)[0]

            self.log.debug(sync_run_details)
            run = utils.parse_sync_run_details(sync_run_details)
            self.log.info(
                f"Polling Hightouch Sync {sync_id}. Current status: {run.status}. "
                f"{100 * run.completion_ratio}% completed."
            )

            if run.status in TERMINAL_STATUSES:
                self.log.info(f"Sync request status: {run.status}. Polling complete")
                if run.error:
                    self.log.info("Sync Request Error: %s", run.error)

                if run.status == SUCCESS:
                    break
                if run.status == WARNING and not fail_on_warning:
                    break
                raise AirflowException(
                    f"Sync {sync_id} for request: {sync_request_id} failed with status: "
                    f"{run.status} and error:  {run.error}"
                )
            if run.status not in PENDING_STATUSES:
                self.log.warning(
                    "Unexpected status: %s returned for sync %s and request %s. Will try "
                    "again, but if you see this error, please let someone at Hightouch know.",
                    run.status,
                    sync_id,
                    sync_request_id,
                )
            if (
                poll_timeout
                and datetime.datetime.now()
                > poll_start + datetime.timedelta(seconds=poll_timeout)
            ):
                raise AirflowException(
                    f"Sync {sync_id} for request: {sync_request_id}' time out after "
                    f"{datetime.datetime.now() - poll_start}. Last status was {run.status}."
                )

            time.sleep(poll_interval)
        sync_details = self.get_sync_details(sync_id)

        return HightouchOutput(sync_details, sync_run_details)

    def sync_and_poll(
        self,
        sync_id: Optional[str] = None,
        sync_slug: Optional[str] = None,
        fail_on_warning: bool = False,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = None,
    ) -> HightouchOutput:
        """
        Initialize a sync run for the given sync id, and polls until it completes
        Args:
            sync_id (str): The Hightouch Sync ID
            sync_request_id (str): The Hightouch Sync Request ID to poll against.
            fail_on_warning (bool): Whether a warning is considered a failure for this sync.
            poll_interval (float): The time in seconds that will be waited between succcessive polls
            poll_timeout (float): The maximum time that will be waited before this operation
                times out.
        Returns:
            :py:class:`~HightouchOutput`:
                Object containing details about the Hightouch sync run
        """
        sync_request_id = self.start_sync(sync_id, sync_slug)

        if not sync_id:
            assert sync_slug
            sync_id = sync_id or self.get_sync_from_slug(sync_slug=sync_slug)

        assert sync_id or sync_slug
        ht_output = self.poll_sync(
            sync_id,
            sync_request_id,
            fail_on_warning=fail_on_warning,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )

        return ht_output
