from typing import Optional

from airflow.models.baseoperator import BaseOperatorLink
from airflow.sensors.base import BaseSensorOperator

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from airflow_provider_hightouch.hooks.hightouch import HightouchHook
from airflow_provider_hightouch.utils import parse_sync_run_details

from airflow_provider_hightouch.consts import *


class HightouchLink(BaseOperatorLink):
    name = "Hightouch"

    def get_link(self, operator, dttm):
        return "https://app.hightouch.io"


class HightouchSyncRunSensor(BaseSensorOperator):
    """
    This operator monitors a specific sync run in Hightouch via the
    Hightouch API.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`https://hightouch.io/docs/integrations/airflow/`

    :param sync_run_id: ID of the sync run to monitor
    :param sync_id: ID of the sync that the sync run belongs to
    :type sync_id: int
    :param connection_id: Name of the connection to use, defaults to hightouch_default
    :type connection_id: str
    :param api_version: Hightouch API version. Only v3 is supported.
    :type api_version: str
    :param error_on_warning: Should sync warnings be treated as errors or ignored?
    :type error_on_warning: bool
    """

    operator_extra_links = (HightouchLink(),)

    @apply_defaults
    def __init__(
        self,
        sync_run_id: str,
        sync_id: str,
        connection_id: str = "hightouch_default",
        api_version: str = "v3",
        error_on_warning: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hightouch_conn_id = connection_id
        self.api_version = api_version
        self.sync_run_id = sync_run_id
        self.sync_id = sync_id
        self.error_on_warning = error_on_warning

    def poke(self, context) -> bool:
        hook = HightouchHook(
            hightouch_conn_id=self.hightouch_conn_id,
            api_version=self.api_version,
        )

        sync_run_details = hook.get_sync_run_details(
            self.sync_id,
            self.sync_run_id
        )[0]

        run = parse_sync_run_details(
            sync_run_details
        )

        if run.status in TERMINAL_STATUSES:
            self.log.info(f"Sync request status: {run.status}.")
            if run.error:
                self.log.info("Sync Request Error: %s", run.error)

            if run.status == SUCCESS:
                return True
            if run.status == WARNING and not self.error_on_warning:
                return True
            raise AirflowException(
                f"Sync {self.sync_id} for request: {self.sync_request_id} failed with status: "
                f"{run.status} and error:  {run.error}"
            )

        return False
