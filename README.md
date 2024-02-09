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

Usage
-----

Set the `CONFIG_PATH` environment variable to the path containing the service config and permission files when starting this service (default: `config`).

Base URL:

    http://localhost:5012/

Service API:

    http://localhost:5012/api/

Sample requests:

    curl 'http://localhost:5012/qwc_demo.edit_points/'

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

## Filter expressions

Query operations support passing filter expressions to narrow down the results.
This expression is a serialized JSON array of the format:

    [["<name>", "<op>", <value>],"and|or",["<name>","<op>",<value>],...]

* `name` is the attribute column name. If `name` begins with `?`, the filter is only applied if the column name exists.
* `op` can be one of

      "=", "!=", "<>", "<", ">", "<=", ">=", "LIKE", "ILIKE", "IS", "IS NOT"

  The operators are applied on the original database types.

  If value is `null`, the operator should be `IS` or `IS NOT`.

* `value` can be of type `string`, `int`, `float` or `null`.

  For string operations, the SQL wildcard character `%` can be used.

### Filter examples

* Find all features in the dataset with a number field smaller 10 and a matching name field:
  `[["name","LIKE","example%"],"and",["number","<",10]]`
* Find all features in the dataset with a last change before 1st of January 2020 or having `NULL` as lastchange value:
  `[["lastchange","<","2020-01-01T12:00:00"],"or",["lastchange","IS",null]]`

Docker usage
------------

To run this docker image you will need a PostGIS database. For testing purposes you can use the demo DB.

The following steps explain how to download the demo DB docker image and how to run the `qwc-data-service` with `docker-compose`.

**Step 1: Clone qwc-docker**

    git clone https://github.com/qwc-services/qwc-docker
    cd qwc-docker

**Step 2: Create docker-compose.yml file**

    cp docker-compose-example.yml docker-compose.yml

**Step 3: Start docker containers**

    docker-compose up qwc-data-service

For more information please visit: https://github.com/qwc-services/qwc-docker

Development
-----------

Create a virtual environment:

    virtualenv --python=/usr/bin/python3 --system-site-packages .venv

Without system packages:

    python3 -m venv .venv

Activate virtual environment:

    source .venv/bin/activate

Install requirements:

    pip install -r requirements.txt

Start local service:

    CONFIG_PATH=/PATH/TO/CONFIGS/ python src/server.py


Testing
-------

Run all tests:

    python test.py

Run single test module:

    python -m unittest tests.feature_validation_tests

Run single test case:

    python -m unittest tests.feature_validation_tests.FeatureValidationTestCase

Run single test method:

    python -m unittest tests.feature_validation_tests.FeatureValidationTestCase.test_field_constraints
