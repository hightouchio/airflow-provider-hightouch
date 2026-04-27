import datetime

from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.common.compat.sdk import DAG

from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator
from airflow_provider_hightouch.sensors.hightouch import HightouchSyncRunSensor

args = {"owner": "airflow"}

with DAG(
    dag_id="example_hightouch_operator",
    default_args=args,
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1),
    dagrun_timeout=datetime.timedelta(minutes=5),
) as dag:

    latest_only = LatestOnlyOperator(task_id="latest_only")

    # This task runs async, and doesn't poll for status or fail on error
    run_async = HightouchTriggerSyncOperator(task_id="run_async", sync_id=4)

    # This tasks polls the API until the Hightouch Sync completes or errors.
    # Warnings are considered errors, but this can be turned off with the
    # specified flag
    run_sync = HightouchTriggerSyncOperator(
        task_id="run_sync", sync_id=5, synchronous=True, error_on_warning=True
    )

    sync_sensor = HightouchSyncRunSensor(
        task_id="sync_sensor",
        sync_run_id="123456",
        sync_id="123",
    )

    latest_only >> sync_sensor
    sync_sensor >> run_sync
    sync_sensor >> run_async