## 3.0.2

### Fixes:

* Fixes issue with repeated warnings when using multiple HTTP connections

## 3.0.1

### Fixes:

* Correctly log parsed Sync run output

## 3.0.0

This is a new API with some breaking changes. Please read the changes carefully
before upgrading!

### NEW:

* Uses the new Hightouch API Endpoint. This endpoint is now idempotent and more
reliable and flexible.

* Can trigger sync via ID or via Slug

* Logs information about the number of rows added, changed, and remove along
with other sync run details

### BREAKING:

* Syncs are now synchronous by default, use `synchronous=False` for async
operations.

## 2.1.2

* #9 Fixes a bug with a missing f in logging unhandled states, and a bug
in an assertion test

## 2.1.1

* Adds support for the queued status from the API

## 2.1.0

* Fixes a bug where starting a sync when a sync is already in progress does not
return the correct sync status

## 2.0.0

* Update Airflow operator to use v2 API

## 1.0.0

* Adds tests and improves provider functionality


## 0.1.0

* Initial release of the provider
