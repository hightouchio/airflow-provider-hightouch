from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults

from airflow_provider_hightouch.hooks.hightouch import HightouchHook
from airflow_provider_hightouch.utils import parse_sync_run_details


class HightouchLink(BaseOperatorLink):
    name = "Hightouch"

    def get_link(self, operator, dttm):
        return "https://app.hightouch.io"


class HightouchTriggerSyncOperator(BaseOperator):
    """
    This operator triggers a run for a specified Sync in Hightouch via the
    Hightouch API.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`https://hightouch.io/docs/integrations/airflow/`

    :param sync_id: ID of the sync to trigger
    :type sync_id: int
    :param sync_slug: Slug of the sync to trigger
    :param connection_id: Name of the connection to use, defaults to hightouch_default
    :type connection_id: str
    :param api_version: Hightouch API version. Only v3 is supported.
    :type api_version: str
    :param synchronous: Whether to wait for the sync to complete before completing the task
    :type synchronous: bool
    :param error_on_warning: Should sync warnings be treated as errors or ignored?
    :type error_on_warning: bool
    :param wait_seconds: Time to wait in between subsequent polls to the API.
    :type wait_seconds: float
    :param timeout: Maximum time to wait for a sync to complete before aborting
    :type timeout: int
    """

    operator_extra_links = (HightouchLink(),)

    @apply_defaults
    def __init__(
        self,
        sync_id: Optional[str] = None,
        sync_slug: Optional[str] = None,
        connection_id: str = "hightouch_default",
        api_version: str = "v3",
        synchronous: bool = True,
        error_on_warning: bool = False,
        wait_seconds: float = 3,
        timeout: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hightouch_conn_id = connection_id
        self.api_version = api_version
        self.sync_id = sync_id
        self.sync_slug = sync_slug
        self.error_on_warning = error_on_warning
        self.synchronous = synchronous
        self.wait_seconds = wait_seconds
        self.timeout = timeout

    def execute(self, context) -> None:
        """Start a Hightouch Sync Run"""
        hook = HightouchHook(
            hightouch_conn_id=self.hightouch_conn_id,
            api_version=self.api_version,
        )

        if not self.sync_id and not self.sync_slug:
            raise AirflowException(
                "One of sync_id or sync_slug must be provided to trigger a sync"
            )

        if self.synchronous:
            self.log.info("Start synchronous request to run a sync.")
            hightouch_output = hook.sync_and_poll(
                self.sync_id,
                self.sync_slug,
                fail_on_warning=self.error_on_warning,
                poll_interval=self.wait_seconds,
                poll_timeout=self.timeout,
            )
            try:
                parsed_result = parse_sync_run_details(
                    hightouch_output.sync_run_details
                )
                self.log.info("Sync completed successfully")
                self.log.info(dict(parsed_result))
            except Exception:
                self.log.warning("Sync ran successfully but failed to parse output.")
                self.log.warning(hightouch_output)

        else:
            self.log.info("Start async request to run a sync.")
            request_id = hook.start_sync(self.sync_id, self.sync_slug)
            sync = self.sync_id or self.sync_slug
            self.log.info(
                "Successfully created request %s to start sync: %s", request_id, sync
            )
