import json

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults

from airflow_provider_hightouch.hooks.hightouch import HightouchHook


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
    :param connection_id: Name of the connection to use, defaults to hightouch_default
    :type connection_id: str
    :param api_version: Hightouch API version. Only v2 is supported.
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
        sync_id: int,
        connection_id: str = "hightouch_default",
        api_version: str = "v2",
        synchronous: bool = False,
        error_on_warning: bool = False,
        wait_seconds: float = 3,
        timeout: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hightouch_conn_id = connection_id
        self.api_version = api_version
        self.sync_id = sync_id
        self.error_on_warning = error_on_warning
        self.synchronous = synchronous
        self.wait_seconds = wait_seconds
        self.timeout = timeout

    def execute(self, context) -> None:
        """Start a Hightouch Sync Run"""
        hook = HightouchHook(hightouch_conn_id=self.hightouch_conn_id)
        result = hook.submit_sync(self.sync_id)

        try:
            message = result.json()
        except json.JSONDecodeError:
            message = result.text

        if result.status_code in (400, 404):
            self.log.error("Bad request received. Does the sync id specified exist?")
            raise AirflowException(message)

        if result.status_code == 403:
            self.log.error(
                "API not authorized. Make sure the password is set to your API token in your Hightouch Airflow connection"
            )
            raise AirflowException(message)

        if result.status_code == 200:
            self.log.info(
                "Successfully sent request to start a run for job: %s", self.sync_id
            )
            if not self.synchronous:
                return message

            latest_sync_run_id = message["latest_sync_run_id"]

            hook.poll(
                self.sync_id,
                self.wait_seconds,
                self.timeout,
                latest_sync_run_id=latest_sync_run_id,
                error_on_warning=self.error_on_warning,
            )

            return message
        self.log.error("Unhandled exception: %s", message)
        raise AirflowException(message)
