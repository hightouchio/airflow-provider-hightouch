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
    run_sync = HightouchTriggerSyncOperator(task_id="run_my_sync", sync_id=1)

    latest_only >> run_sync
