# Apache Airflow Provider for Hightouch

Provides an Airflow Operator and Hook for [Hightouch](https://hightouch.io).
This allows the user to initiate a run for a sync from Airflow.

## Installation

Pre-requisites: An environment running `apache-airflow` >= 1.10, including >= 2.

```
pip install airflow-provider-hightouch
```

## Configuration

In the Airflow Connections UI, create a new connection for Hightouch.

* `Conn ID`: `hightouch_default`
* `Conn Type`: `HTTP`
* `Host`: `https://api.hightouch.io`
* `Password`: enter the API key for your workspace.  You can generate an API
key from your [Workspace Settings](https://app.hightouch.io/settings)

The Operator uses the `hightouch_default` connection id by default, but
if needed, you can create additional Airflow Connections and reference them
in the operator


## Modules

### [HightouchTriggerSyncOperator](./airflow_provider_hightouch/operators/hightouch.py)

Starts a Hightouch Sync Run. Requires the `sync_id` for the sync you wish to
run. You can find the `sync_id` in the browser url: `https://app.hightouch.io/syncs/[your-sync-id]`

The run is asynchronous by default, and the task will be marked complete if the request
was successfully sent to the Hightouch API.

However, you can request a synchronous request instead by passing `synchronous=True`
to the operator.

If the API key is not authorized or if the request is invalid the task will fail.
If a run is already in progress, a new run will be triggered following the
completion of the existing run.


## Examples

Creating a run is as simple as importing the operator and providing it with
a sync_id. An [example dag](./airflow_provider_hightouch/example_dags/example_hightouch_trigger_sync.py)
is available as well.

```
from airflow_provider_hightouch.operators.hightouch import HightouchTriggerSyncOperator

with DAG(....) as dag:
...
    my_task = HightouchTriggerSyncOperator(task_id="run_my_sync", sync_id=1)

```

## Issues

Please submit [issues](https://github.com/hightouchio/airflow-provider-hightouch/issues) and
[pull requests](https://github.com/hightouchio/airflow-provider-hightouch/pulls) in our official repo:
[https://github.com/hightouchio/airflow-provider-hightouch](https://github.com/hightouchio/airflow-provider-hightouch)

We are happy to hear from you, for any feedback please email the authors at [pedram@hightouch.io](mailto:pedram@hightouch.io).

## Acknowledgements

Special thanks to [Fivetran](https://github.com/fivetran/airflow-provider-fivetran)
for their provider and [Marcos Marx](https://github.com/marcosmarxm/)'s Airbyte
contribution in the core Airflow repo for doing this before we had to
so we could generously learn from their hard work.
