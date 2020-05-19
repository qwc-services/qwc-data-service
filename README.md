[![](https://github.com/qwc-services/qwc-data-service/workflows/build/badge.svg)](https://github.com/qwc-services/qwc-data-service/actions)
[![](https://img.shields.io/docker/pulls/sourcepole/qwc-data-service)](https://hub.docker.com/repository/docker/sourcepole/qwc-data-service)

QWC Data Service v2
===================

Edit spatial and unlocated features of datasets via GeoJSON.

**v2** (WIP): add support for multitenancy and replace QWC Config service with static config and permission files.

**Note:**: Filter expressions have been refactored to JSON serialized arrays in v2.

**Note:** requires a PostGIS database for reading and writing features

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
* File location: `$CONFIG_PATH/<tenant>/dataConfig.json`

Example:
```json
{
  "$schema": "https://raw.githubusercontent.com/qwc-services/qwc-data-service/v2/schemas/qwc-data-service.json",
  "service": "data",
  "config": {},
  "resources": {
    "datasets": [
      {
        "name": "qwc_demo.edit_points",
        "db_url": "postgresql:///?service=qwc_geodb",
        "schema": "qwc_geodb",
        "table_name": "edit_points",
        "primary_key": "id",
        "fields": [
          {
            "name": "id",
            "data_type": "integer",
            "constraints": {
              "min": -2147483648,
              "max": 2147483647
            }
          },
          {
            "name": "name",
            "data_type": "character varying",
            "constraints": {
              "maxlength": 32
            }
          },
          {
            "name": "description",
            "data_type": "text"
          },
          {
            "name": "num",
            "data_type": "integer",
            "constraints": {
              "min": -2147483648,
              "max": 2147483647
            }
          },
          {
            "name": "value",
            "data_type": "double precision",
            "constraints": {
              "pattern": "[0-9]+([\\.,][0-9]+)?"
            }
          },
          {
            "name": "type",
            "data_type": "smallint",
            "constraints": {
              "min": -32768,
              "max": 32767
            }
          },
          {
            "name": "amount",
            "data_type": "numeric",
            "constraints": {
              "numeric_precision": 5,
              "numeric_scale": 2,
              "min": -999.99,
              "max": 999.99,
              "step": 0.01
            }
          },
          {
            "name": "validated",
            "data_type": "boolean"
          },
          {
            "name": "datetime",
            "data_type": "timestamp without time zone"
          }
        ],
        "geometry": {
          "geometry_column": "geom",
          "geometry_type": "POINT",
          "srid": 3857
        }
      }
    ]
  }
}
```

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

### Overview

                                     QWC2 build:                                      Public:
    +--------------------+           +------------+
    | themesConfig.json  |           |            |
    |          ^         +-----------> yarn build +----------------------------------> (themes.json)
    |          |         |       +--->            |
    |          +         +-----+ |   +------------+       +---------------+
    |        (edit.json) |     | |                        |               |
    +--------------------+     | +------------------------+ QGIS Server   +----------> WMS/WFS
                               | |      Capabilities      |               |
    +--------------------+     | |                        +------^--------+
    | ui Files           +-----------------------------------------------------------> assets/*.ui
    +--------------------+     | |                               |
                               | |                               |                    +------------------+
    +--------------------+     | |                               +                    |                  +--> html/js/css/assets
    | qgs Files          +--+-------------------------------> *.qgs         +---------> qwc-map-viewer   |
    +--------------------+  |  | |                                          |         |                  +--> config.json/themes.json
                            |  | |                                          |         +------------------+
                            |  | |                                          |
                            |  | |                                          |         +------------------+
                            |  | |                                          |         |                  |  REST-API
        +---------+         |  | |                                          +---------> qwc-data-service <--------------->
        |         |         |  | |   +------------+                         |         |                  |
        | config- |         |  | +--->            |                         +         +------------------+
        |   DB    | +-------v--v-----> Config-    +------> [service].json+permissions.json
        |         |                  | Generator  |
        |         |                  |            |
        +---------+                  +------------+

Edit forms:

- Edit forms are automatically created from field information extracted from QGS files
- ui-Files created with QT Designer can also be used to create edit forms

Data service configuration:

- DB connection information, table and column names and primary key information are extracted from QGS files
- Data contraints are extracted from QGS files
- Column types and additional constraints are read from the the geo-DB

Data read/write:

- QWC2 issues data-service API requests for reading und writing


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

    virtualenv --python=/usr/bin/python3 .venv

Activate virtual environment:

    source .venv/bin/activate

Install requirements:

    pip install -r requirements.txt

Start local service:

    CONFIG_PATH=/PATH/TO/CONFIGS/ python server.py


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
