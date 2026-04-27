## 5.0.0

### BREAKING:

* Drops support for Apache Airflow 2.x. Requires Airflow 3.0.0+.
* Requires Python 3.10+.
* Requires `apache-airflow-providers-http` as an explicit dependency (bundled in Airflow 2, separate in Airflow 3).

### Changes:

* Updates all imports to use `airflow.sdk` canonical paths (BaseOperator, BaseSensorOperator, BaseOperatorLink).
* Removes `@apply_defaults` decorator (removed in Airflow 3).
* Updates `HightouchLink.get_link()` to use `ti_key: TaskInstanceKey` keyword argument.
* Removes dead Airflow 1.x import fallback in hook.
* Consolidates duplicate `HightouchLink` class (now defined only in operators module).
* Replaces wildcard import in sensor with explicit imports.
* Adds `template_fields` to `HightouchTriggerSyncOperator` for Jinja templating of `sync_id` and `sync_slug`.
* Fixes example DAG for Airflow 3 compatibility.
* Adds sensor tests.

## 4.0.0

- Introduces HightouchSyncRunSensor, which monitors the success or failure of a sync run
- Adds a return value to HightouchTriggerSyncOperator

## 3.0.2

### Fixes:

- Fixes issue with repeated warnings when using multiple HTTP connections

## 3.0.1

### Fixes:

- Correctly log parsed Sync run output

## 3.0.0

This is a new API with some breaking changes. Please read the changes carefully
before upgrading!

### NEW:

- Uses the new Hightouch API Endpoint. This endpoint is now idempotent and more
  reliable and flexible.

- Can trigger sync via ID or via Slug

- Logs information about the number of rows added, changed, and remove along
  with other sync run details

### BREAKING:

- Syncs are now synchronous by default, use `synchronous=False` for async
  operations.

## 2.1.2

- #9 Fixes a bug with a missing f in logging unhandled states, and a bug
  in an assertion test

## 2.1.1

- Adds support for the queued status from the API

## 2.1.0

- Fixes a bug where starting a sync when a sync is already in progress does not
  return the correct sync status

## 2.0.0

- Update Airflow operator to use v2 API

## 1.0.0

- Adds tests and improves provider functionality

## 0.1.0

- Initial release of the provider
