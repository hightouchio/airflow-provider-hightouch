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
        :ref:`#TODO: add link to docs`

    :param api_version: Optional. Hightouch API version
    :type api_version: str
    """

    operator_extra_links = (HightouchLink(),)

    @apply_defaults
    def __init__(
        self,
        sync_id: int,
        connection_id: str = "hightouch_default",
        api_version: str = "v1",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hightouch_conn_id = connection_id
        self.api_version = api_version
        self.sync_id = sync_id

    def execute(self, context) -> None:
        """Start a Hightouch Sync Run"""
        hook = HightouchHook(hightouch_conn_id=self.hightouch_conn_id)
        result = hook.submit_sync(self.sync_id)

        try:
            message = result.json()
        except json.JSONDecodeError:
            message = result.text

        if result.status_code == 200:
            self.log.info(
                "Successfully sent request to start a run for job: %s", self.sync_id
            )
            return message

        if result.status_code == 403:
            self.log.error(
                "API not authorized. Make sure the password is set to your API token in your Hightouch Airflow connection"
            )
            raise AirflowException(message)

        if result.status_code == 400:
            self.log.error("Bad request received. Does the sync id specified exist?")
            raise AirflowException(message)

        self.log.error("Unhandled exception: %s", message)
        raise AirflowException(message)
