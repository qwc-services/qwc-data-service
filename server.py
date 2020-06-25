from collections import OrderedDict
from datetime import date
from decimal import Decimal
from uuid import UUID

from flask import Flask, Request as RequestBase, request, jsonify
from flask_restx import Api, Resource, fields, reqparse
from flask_jwt_extended import JWTManager, jwt_optional, get_jwt_identity
from werkzeug.exceptions import BadRequest

from qwc_services_core.api import create_model, CaseInsensitiveArgument
from qwc_services_core.jwt import jwt_manager
from qwc_services_core.tenant_handler import TenantHandler
from data_service import DataService


class Request(RequestBase):
    """Custom Flask Request subclass"""
    def on_json_loading_failed(self, e):
        """Always return detailed JSON decode error, not only in debug mode"""
        raise BadRequest('Failed to decode JSON object: {0}'.format(e))


class FeatureProperties(fields.Raw):
    """Custom Flask-RESTPlus Field for feature properties"""
    def format(self, properties):
        """Formats feature property values to be JSON serializable."""
        res = OrderedDict()
        for attr, value in properties.items():
            if isinstance(value, date):
                res[attr] = value.isoformat()
            elif isinstance(value, Decimal):
                res[attr] = float(value)
            elif isinstance(value, UUID):
                res[attr] = str(value)
            else:
                res[attr] = value

        return res


# Flask application
app = Flask(__name__)
# use custom Request subclass
app.request_class = Request
# Flask-RESTPlus Api
api = Api(app, version='1.0', title='Data service API',
          description="""API for QWC Data service.

## General Information for all operations

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

* `name` is the attribute column name
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
          """,
          default_label='Data edit operations', doc='/api/'
          )
# Omit X-Fields header in docs
app.config['RESTPLUS_MASK_SWAGGER'] = False
# disable verbose 404 error message
app.config['ERROR_404_HELP'] = False

# Setup the Flask-JWT-Extended extension
jwt = jwt_manager(app, api)

# create tenant handler
tenant_handler = TenantHandler(app.logger)


def data_service_handler():
    """Get or create a DataService instance for a tenant."""
    tenant = tenant_handler.tenant()
    handler = tenant_handler.handler('data', 'data', tenant)
    if handler is None:
        handler = tenant_handler.register_handler(
            'data', tenant, DataService(tenant, app.logger))
    return handler


# Api models
geojson_crs_properties = create_model(api, 'CRS Properties', [
    ['name', fields.String(required=True, description='OGC CRS URN',
                           example='urn:ogc:def:crs:EPSG::3857')],
])

geojson_crs = create_model(api, 'CRS', [
    ['type', fields.String(required=True, description='CRS type',
                           example='name')],
    ['properties', fields.Nested(geojson_crs_properties, required=True,
                                 description='CRS properties')]
])

geojson_geometry = create_model(api, 'Geometry', [
    ['type', fields.String(required=True, description='Geometry type',
                           example='Point')],
    ['coordinates', fields.Raw(required=True, description='Coordinates',
                               example=[950598.0, 6004010.0])]
])

# Feature response
geojson_feature_response = create_model(api, 'Feature', [
    ['type', fields.String(required=True, description='Feature',
                           example='Feature')],
    ['id', fields.Integer(required=True, description='Feature ID',
                          example=123)],
    ['geometry', fields.Nested(geojson_geometry, required=False,
                               allow_null=True,
                               description='Feature geometry')],
    ['properties', FeatureProperties(required=True,
                                     description='Feature properties',
                                     example={'name': 'Example', 'type': 2,
                                              'num': 4}
                                     )],
    ['crs', fields.Nested(geojson_crs, required=False, allow_null=True,
                          description='Coordinate reference system')],
    ['bbox', fields.Raw(required=False, allow_null=True,
                        description=(
                            'Extent of feature as [minx, miny, maxx, maxy]'
                        ),
                        example=[950598.0, 6003950.0, 950758.0, 6004010.0])]
])

# Feature request
# NOTE: 'id' field not included, as ID is always defined by route
geojson_feature_request = create_model(api, 'Input Feature', [
    ['type', fields.String(required=True, description='Feature',
                           example='Feature')],
    ['geometry', fields.Nested(geojson_geometry, required=False,
                               allow_null=True,
                               description='Feature geometry')],
    ['properties', fields.Raw(required=True, description='Feature properties',
                              example={'name': 'Example', 'type': 2, 'num': 4}
                              )],
    ['crs', fields.Nested(geojson_crs, required=False, allow_null=True,
                          description='Coordinate reference system')]
])

# FeatureCollection response
# NOTE: 'crs' field already defined by parent FeatureCollection
geojson_feature_member = create_model(api, 'Member Feature', [
    ['type', fields.String(required=True, description='Feature',
                           example='Feature')],
    ['id', fields.Integer(required=True, description='Feature ID',
                          example=123)],
    ['geometry', fields.Nested(geojson_geometry, required=False,
                               allow_null=True,
                               description='Feature geometry')],
    ['properties', FeatureProperties(required=True,
                                     description='Feature properties',
                                     example={'name': 'Example', 'type': 2,
                                              'num': 4}
                                     )]
])

geojson_feature_collection_response = create_model(api, 'FeatureCollection', [
    ['type', fields.String(required=True, description='FeatureCollection',
                           example='FeatureCollection')],
    ['features', fields.List(fields.Nested(geojson_feature_member),
                             required=True, description='Features')],
    ['crs', fields.Nested(geojson_crs, required=False, allow_null=True,
                          description='Coordinate reference system')],
    ['bbox', fields.Raw(required=False, allow_null=True,
                        description=(
                            'Extent of features as [minx, miny, maxx, maxy]'
                        ),
                        example=[950598.0, 6003950.0, 950758.0, 6004010.0])]
])

# message response
message_response = create_model(api, 'Message', [
    ['message', fields.String(required=True, description='Response message',
                              example='Dataset feature deleted')]
])

# feature validation error response
geometry_error = create_model(api, 'Geometry error', [
    ['reason', fields.String(required=True, description='Description',
                             example='Self-intersection')],
    ['location', fields.String(required=False, description='Location as WKT',
                               example='POINT(950598.0 6004010.0)')]
])

feature_validation_response = create_model(api, 'Feature validation error', [
    ['message', fields.String(required=True, description='Error message',
                              example='Feature validation failed')],
    ['validation_errors', fields.List(fields.String(), required=False,
                                      description='Feature validation errors',
                                      example=['Missing GeoJSON geometry'])],
    ['geometry_errors', fields.List(fields.Nested(geometry_error),
                                    required=False,
                                    description='Geometry validation errors')]
])


# request parser
index_parser = reqparse.RequestParser(argument_class=CaseInsensitiveArgument)
index_parser.add_argument('bbox')
index_parser.add_argument('crs')
index_parser.add_argument('filter')

show_parser = reqparse.RequestParser(argument_class=CaseInsensitiveArgument)
show_parser.add_argument('crs')

get_relations_parser = reqparse.RequestParser(argument_class=CaseInsensitiveArgument)
get_relations_parser.add_argument('tables', required=True)


# routes
@api.route('/<dataset>/')
@api.response(400, 'Bad request')
@api.response(404, 'Dataset not found or permission error')
@api.param('dataset', 'Dataset ID', default='qwc_demo.edit_points')
class DataCollection(Resource):
    @api.doc('index')
    @api.response(405, 'Dataset not readable')
    @api.param('bbox', 'Bounding box as `<minx>,<miny>,<maxx>,<maxy>`')
    @api.param('crs', 'Client coordinate reference system, e.g. `EPSG:3857`')
    @api.param(
        'filter', 'JSON serialized array of filter expressions: '
        '`[["<name>", "<op>", <value>],"and|or",["<name>","<op>",<value>]]`')
    @api.expect(index_parser)
    @api.marshal_with(geojson_feature_collection_response, skip_none=True)
    @jwt_optional
    def get(self, dataset):
        """Get dataset features

        Return dataset features inside bounding box and matching filter as a
        GeoJSON FeatureCollection.
        """
        args = index_parser.parse_args()
        bbox = args['bbox']
        crs = args['crs']
        filterexpr = args['filter']

        data_service = data_service_handler()
        result = data_service.index(
            get_jwt_identity(), dataset, bbox, crs, filterexpr
        )
        if 'error' not in result:
            return result['feature_collection']
        else:
            error_code = result.get('error_code') or 404
            api.abort(error_code, result['error'])

    @api.doc('create')
    @api.response(405, 'Dataset not creatable')
    @api.response(422, 'Feature validation failed', feature_validation_response)
    @api.expect(geojson_feature_request)
    @api.marshal_with(geojson_feature_response, code=201)
    @jwt_optional
    def post(self, dataset):
        """Create a new dataset feature

        Create new dataset feature from a GeoJSON Feature and return it as a
        GeoJSON Feature.
        """
        if request.is_json:
            # parse request data (NOTE: catches invalid JSON)
            payload = api.payload
            if isinstance(payload, dict):
                data_service = data_service_handler()
                result = data_service.create(
                    get_jwt_identity(), dataset, payload)
                if 'error' not in result:
                    return result['feature'], 201
                else:
                    error_code = result.get('error_code') or 404
                    error_details = result.get('error_details') or {}
                    api.abort(error_code, result['error'], **error_details)
            else:
                api.abort(400, "JSON is not an object")
        else:
            api.abort(400, "Request data is not JSON")


@api.route('/<dataset>/<int:id>')
@api.response(404, 'Dataset or feature not found or permission error')
@api.param('dataset', 'Dataset ID', default='qwc_demo.edit_points')
@api.param('id', 'Feature ID')
class DataMember(Resource):
    @api.doc('show')
    @api.response(405, 'Dataset not readable')
    @api.param('crs', 'Client coordinate reference system')
    @api.expect(show_parser)
    @api.marshal_with(geojson_feature_response)
    @jwt_optional
    def get(self, dataset, id):
        """Get a dataset feature

        Return dataset feature with ID as a GeoJSON Feature.

        Query parameter:

        <b>crs</b>: Client CRS, e.g. <b>EPSG:3857<b>
        """
        args = show_parser.parse_args()
        crs = args['crs']

        data_service = data_service_handler()
        result = data_service.show(get_jwt_identity(), dataset, id, crs)
        if 'error' not in result:
            return result['feature']
        else:
            api.abort(404, result['error'])

    @api.doc('update')
    @api.response(400, 'Bad request')
    @api.response(405, 'Dataset not updatable')
    @api.response(422, 'Feature validation failed', feature_validation_response)
    @api.expect(geojson_feature_request)
    @api.marshal_with(geojson_feature_response)
    @jwt_optional
    def put(self, dataset, id):
        """Update a dataset feature

        Update dataset feature with ID from a GeoJSON Feature and return it as
        a GeoJSON Feature.
        """
        if request.is_json:
            # parse request data (NOTE: catches invalid JSON)
            payload = api.payload
            if isinstance(payload, dict):
                data_service = data_service_handler()
                result = data_service.update(
                    get_jwt_identity(), dataset, id, api.payload
                )
                if 'error' not in result:
                    return result['feature']
                else:
                    error_code = result.get('error_code') or 404
                    error_details = result.get('error_details') or {}
                    api.abort(error_code, result['error'], **error_details)
            else:
                api.abort(400, "JSON is not an object")
        else:
            api.abort(400, "Request data is not JSON")

    @api.doc('destroy')
    @api.response(405, 'Dataset not deletable')
    @api.marshal_with(message_response)
    @jwt_optional
    def delete(self, dataset, id):
        """Delete a dataset feature

        Delete dataset feature with ID.
        """
        data_service = data_service_handler()
        result = data_service.destroy(get_jwt_identity(), dataset, id)
        if 'error' not in result:
            return {
                'message': "Dataset feature deleted"
            }
        else:
            error_code = result.get('error_code') or 404
            api.abort(error_code, result['error'])

@api.route('/<dataset>/<int:id>/relations')
@api.response(404, 'Dataset or feature not found or permission error')
@api.param('dataset', 'Dataset ID', default='qwc_demo.edit_points')
@api.param('id', 'Feature ID')
class Relations(Resource):
    @api.doc('get_relations')
    @api.param('tables', 'Comma separated list of relation tables of the form "tablename:fk_field_name"')
    @api.expect(get_relations_parser)
    # TODO
    #@api.marshal_with(relationvalues_response, code=201)
    def get(self, dataset, id):
        data_service = data_service_handler()
        args = get_relations_parser.parse_args()
        relations = args['tables'] or ""
        ret = {}
        for relation in relations.split(","):
            try:
                table, fk_field_name = relation.split(":")
            except:
                continue
            ret[table] = {"fk": fk_field_name, "records": []}
            result = data_service.index(
                get_jwt_identity(), table, None, None, "%s = %d" % (fk_field_name, id)
            )
            if 'feature_collection' in result:
                for feature in result['feature_collection']['features']:
                    record = {(table + "__" + k): v for k, v in feature['properties'].items()}
                    record["id"] = feature["id"]
                    ret[table]['records'].append(record)
        return {"relationvalues": ret}

    @api.doc('post_relations')
    # TODO
    #@api.expect(relationvalues_request)
    #@api.marshal_with(relationvalues_response, code=201)
    @jwt_optional
    def post(self, dataset, id):
        """Update relation values for the specified dataset

        Return success status for each relation value.
        """
        if not request.is_json:
            api.abort(400, "Request data is not JSON")

        payload = api.payload
        if not isinstance(payload, dict):
            api.abort(400, "JSON is not an object")

        data_service = data_service_handler()

        # Check if dataset with specified id exists
        if not data_service.is_editable(get_jwt_identity(), dataset, id):
            api.abort(404, "Dataset or feature not found or permission error")

        ret = {}
        haserrors = False
        for (rel_table, rel_data) in payload.items():
            fk_field = rel_data.get("fk", None)
            ret[rel_table] = {
                "fk": fk_field,
                "records": []
            }
            tbl_prefix = rel_table + "__"
            for rel_record in rel_data.get("records", []):
                # Set foreign key for new records
                if rel_record.get("__status__", "") == "new":
                    rel_record[tbl_prefix + fk_field] = id

                if rel_record.get(tbl_prefix + fk_field, None) != id:
                    rel_record["__error__"] = "FK validation failed"
                    ret[rel_table]["records"].append(rel_record)
                    haserrors = True
                else:
                    json = {
                        "type": "Feature",
                        "id": rel_record["id"] if "id" in rel_record else None,
                        "properties": {k[len(tbl_prefix):]: v for k, v in rel_record.items() if k.startswith(tbl_prefix)}
                    }
                    if not "__status__" in rel_record:
                        ret[rel_table]["records"].append(rel_record)
                        continue
                    elif rel_record["__status__"] == "new":
                        result = data_service.create(get_jwt_identity(), rel_table, json)
                    elif rel_record["__status__"] == "changed":
                        result = data_service.update(get_jwt_identity(), rel_table, rel_record["id"], json)
                    elif rel_record["__status__"].startswith("deleted"):
                        result = data_service.destroy(get_jwt_identity(), rel_table, rel_record["id"])
                    else:
                        continue
                    if "error" in result:
                        rel_record["error"] = result["error"]
                        rel_record["error_details"] = result.get('error_details') or {}
                        ret[rel_table]["records"].append(rel_record)
                        haserrors = True
                    elif "feature" in result:
                        rel_record = {(rel_table + "__" + k): v for k, v in result['feature']['properties'].items()}
                        rel_record["id"] = result['feature']["id"]
                        ret[rel_table]["records"].append(rel_record)
        return {"relationvalues": ret, "success": not haserrors}


@api.route('/keyvals')
@api.response(404, 'Dataset or feature not found or permission error')
class KeyValues(Resource):
    @api.doc('get_relations')
    @api.param('tables', 'Comma separated list of keyvalue tables of the form "tablename:key_field_name:value_field_name"')
    @api.expect(get_relations_parser)
    # TODO
    #@api.marshal_with(relationvalues_response, code=201)
    def get(self):
        args = get_relations_parser.parse_args()

        data_service = data_service_handler()

        keyvals = args['tables'] or ""
        ret = {}
        for keyval in keyvals.split(","):
            try:
                table, key_field_name, value_field_name = keyval.split(":")
            except:
                continue
            ret[table] = []
            result = data_service.index(
                get_jwt_identity(), table, None, None, None
            )
            if 'feature_collection' in result:
                for feature in result['feature_collection']['features']:
                    record = {"key": feature["id"] if key_field_name == "id" else feature['properties'][key_field_name], "value": feature['properties'][value_field_name].strip()}
                    ret[table].append(record)
        return {"keyvalues": ret}


""" readyness probe endpoint """
@app.route("/ready", methods=['GET'])
def ready():
    return jsonify({"status": "OK"})


""" liveness probe endpoint """
@app.route("/healthz", methods=['GET'])
def healthz():
    return jsonify({"status": "OK"})


# local webserver
if __name__ == '__main__':
    print("Starting Data service...")
    app.run(host='localhost', port=5012, debug=True)
