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
