from datetime import timedelta

from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.dates import days_ago

from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator

args = {"owner": "airflow"}

with DAG(
    dag_id="example_hightouch_operator",
    default_args=args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=5),
) as dag:

    latest_only = LatestOnlyOperator(task_id="latest_only", dag=dag)

    # This task runs async, and doesn't poll for status or fail on error
    run_async = HightouchTriggerSyncOperator(task_id="run_async", sync_id=4)

    # This tasks polls the API until the Hightouch Sync completes or errors.
    # Warnings are considered errors, but this can be turned off with the
    # specified flag
    run_sync = HightouchTriggerSyncOperator(
        task_id="run_sync", sync_id=5, synchronous=True, error_on_warning=True
    )

    latest_only >> run_sync
    latest_only >> run_async
