[![](https://github.com/qwc-services/qwc-data-service/workflows/build/badge.svg)](https://github.com/qwc-services/qwc-data-service/actions)
[![docker](https://img.shields.io/docker/v/sourcepole/qwc-data-service?label=Docker%20image&sort=semver)](https://hub.docker.com/r/sourcepole/qwc-data-service)

QWC Data Service
================

Edit spatial and unlocated features of datasets via GeoJSON.

**This service is integrated into `qwc-docker`, consult [qwc-services.github.io](https://qwc-services.github.io/) for the general `qwc-services` documentation.**

Setup
-----

Uses PostgreSQL connection service or connection to a PostGIS database.
This connection's user requires read and write access to the configured tables.

### qwc_demo example

Uses PostgreSQL connection service `qwc_geodb` (GeoDB).
The user `qwc_service_write` requires read and write access to the configured tables
of the data layers from the QGIS project `qwc_demo.qgs`.

Setup PostgreSQL connection service file `~/.pg_service.conf`:

```
[qwc_geodb]
host=localhost
port=5439
dbname=qwc_demo
user=qwc_service_write
password=qwc_service_write
sslmode=disable
```


Configuration
-------------

The static config and permission files are stored as JSON files in `$CONFIG_PATH` with subdirectories for each tenant,
e.g. `$CONFIG_PATH/default/*.json`. The default tenant name is `default`.


### Data Service config

* [JSON schema](schemas/qwc-data-service.json)
* [Example `dataConfig.json`](https://github.com/qwc-services/qwc-docker/blob/master/volumes/config/default/dataConfig.json)

### Environment variables

Config options in the config file can be overridden by equivalent uppercase environment variables.

In addition, the following environment variables are supported:

| Name                     | Default       | Description                                                                               |
|--------------------------|---------------|-------------------------------------------------------------------------------------------|
| `ERROR_DETAILS_LOG_ONLY` | `False`       | Whether to omit detailed errors in API responses, and write these only to the service log.|


### Permissions

* [JSON schema](https://github.com/qwc-services/qwc-services-core/blob/master/schemas/qwc-services-permissions.json)
* File location: `$CONFIG_PATH/<tenant>/permissions.json`

Example:
```json
{
  "$schema": "https://raw.githubusercontent.com/qwc-services/qwc-services-core/master/schemas/qwc-services-permissions.json",
  "users": [
    {
      "name": "demo",
      "groups": ["demo"],
      "roles": []
    }
  ],
  "groups": [
    {
      "name": "demo",
      "roles": ["demo"]
    }
  ],
  "roles": [
    {
      "role": "public",
      "permissions": {
        "data_datasets": [
          {
            "name": "qwc_demo.edit_points",
            "attributes": [
              "id",
              "name",
              "description",
              "num",
              "value",
              "type",
              "amount",
              "validated",
              "datetime"
            ],
            "writable": true,
            "creatable": true,
            "readable": true,
            "updatable": true,
            "deletable": true
          }
        ]
      }
    }
  ]
}
```

Run locally
-----------

Install dependencies and run:

    # Setup venv
    uv venv .venv

    export CONFIG_PATH=<CONFIG_PATH>
    uv run src/server.py

To use configs from a `qwc-docker` setup, set `CONFIG_PATH=<...>/qwc-docker/volumes/config`.

Set `FLASK_DEBUG=1` for additional debug output.

Set `FLASK_RUN_PORT=<port>` to change the default port (default: `5000`).

API documentation:

    http://localhost:$FLASK_RUN_PORT/api/

Docker usage
------------

The Docker image is published on [Dockerhub](https://hub.docker.com/r/sourcepole/qwc-data-service).

See sample [docker-compose.yml](https://github.com/qwc-services/qwc-docker/blob/master/docker-compose-example.yml) of [qwc-docker](https://github.com/qwc-services/qwc-docker).


General Information for all operations
--------------------------------------

### Datatypes-Encoding

JSON only defines recommendations or has no information concerning
the encoding of some quite common used database data types.
Following a description on how these are encoded in the data
service API.

- Date: ISO date strings `YYYY-MM-DD`
- Datetime: ISO date/time strings `YYYY-MM-DDThh:mm:ss`
- UUID: Hex-encoded string format. Example: `'6fa459ea-ee8a-3ca4-894e-db77e160355e'`

### Feature-ID

For operations like updating or deleting features, records are identified by
a feature `id`. This `id` refers to the primary key of the database
table and is usually kept constant over time.

### Filter expressions

Query operations support passing filter expressions to narrow down the results.
This expression is a serialized JSON array of the format:

    [["<name>", "<op>", <value>],"and|or",["<name>","<op>",<value>],...]

* `name` is the attribute column name. If `name` begins with `?`, the filter is only applied if the column name exists.
* `op` can be one of

      "=", "!=", "<>", "<", ">", "<=", ">=", "~", "LIKE", "ILIKE", "IS", "IS NOT", "HAS"

  The operators are applied on the original database types.

  The operator `~` is a shorthand for `ILIKE`, the `HAS` and `HAS NOT` operators is used to check if an array field contains / does not contain a value.

  If value is `null`, the operator should be `IS` or `IS NOT`.

* `value` can be of type `string`, `int`, `float` or `null`.

  For string operations, the SQL wildcard character `%` can be used.

*Examples:*

* Find all features in the dataset with a number field smaller 10 and a matching name field:
  `[["name","LIKE","example%"],"and",["number","<",10]]`
* Find all features in the dataset with a last change before 1st of January 2020 or having `NULL` as lastchange value:
  `[["lastchange","<","2020-01-01T12:00:00"],"or",["lastchange","IS",null]]`


Testing
-------

    # Run all tests
    python test.py

    # Run single test module
    python -m unittest tests.feature_validation_tests

    # Run single test case
    python -m unittest tests.feature_validation_tests.FeatureValidationTestCase

    # Run single test method
    python -m unittest tests.feature_validation_tests.FeatureValidationTestCase.test_field_constraints
