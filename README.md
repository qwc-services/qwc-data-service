QWC Data Service v2
===================

Edit spatial and unlocated features of datasets via GeoJSON.

**v2** (WIP): add support for multitenancy and replace QWC Config service with static config and permission files.

**Note:**: Filter expressions have been refactored to JSON serialized arrays in v2.

**Note:** requires a PostGIS database for reading and writing features

Docker usage
------------

### Run docker image

    docker run -v /PATH/TO/pg_service.conf:/var/www/.pg_service.conf:ro \
      -v /PATH/TO/CONFIG:/etc/qwc_service/:ro \
      -e "CONFIG_PATH=/etc/qwc_service" \
      -p 5012:9090 sourcepole/qwc-data-service

A configuration database is required to run the Data Service. For testing purposes you can use the demo DB:

    docker run -p 5439:5432 sourcepole/qwc-demo-db


| docker parameters | Description |
|----------------------|-------------|
| `-v /PATH/TO/pg_service.conf:/var/www/.pg_service.conf:ro` | Mount your pg_service.conf file to `/var/www/` with read only mode|
| `-v /PATH/TO/CONFIG:/etc/qwc_service/:ro` | Mount your config directory to `/etc/qwc_service/` with read only mode|
| `-e "CONFIG_PATH=/etc/qwc_service"` | Set enviroment varible [CONFIG_PATH](#Configuration)|
| `-p 5012:9090` | This binds port 9090 of the container to port 5012 on 127.0.0.1 of the host machine. |

### Build docker image locally:

    docker build .

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
        "database": "postgresql:///?service=qwc_geodb",
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

* File location: `$CONFIG_PATH/<tenant>/permissions.json`

Example:
```json
{
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
