from collections import OrderedDict
from datetime import date
from decimal import Decimal
from uuid import UUID

from flask import Flask, Request as RequestBase, request
from flask_restplus import Api, Resource, fields, reqparse
from flask_jwt_extended import JWTManager, jwt_optional, get_jwt_identity
from werkzeug.exceptions import BadRequest

from qwc_services_core.api import create_model, CaseInsensitiveArgument
from qwc_services_core.jwt import jwt_manager
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
          description='API for QWC Data service',
          default_label='Data edit operations', doc='/api/',
          )
# disable verbose 404 error message
app.config['ERROR_404_HELP'] = False

# Setup the Flask-JWT-Extended extension
jwt = jwt_manager(app, api)

# create data service
data_service = DataService(app.logger)


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
    ['geometry', fields.Nested(geojson_geometry, required=True,
                               description='Feature geometry')],
    ['properties', FeatureProperties(required=True,
                                     description='Feature properties',
                                     example={'name': 'Example', 'type': 2,
                                              'num': 4}
                                     )],
    ['crs', fields.Nested(geojson_crs, required=True,
                          description='Coordinate reference system')]
])

# Feature request
# NOTE: 'id' field not included, as ID is always defined by route
geojson_feature_request = create_model(api, 'Input Feature', [
    ['type', fields.String(required=True, description='Feature',
                           example='Feature')],
    ['geometry', fields.Nested(geojson_geometry, required=True,
                               description='Feature geometry')],
    ['properties', fields.Raw(required=True, description='Feature properties',
                              example={'name': 'Example', 'type': 2, 'num': 4}
                              )],
    ['crs', fields.Nested(geojson_crs, required=True,
                          description='Coordinate reference system')]
])

# FeatureCollection response
# NOTE: 'crs' field already defined by parent FeatureCollection
geojson_feature_member = create_model(api, 'Member Feature', [
    ['type', fields.String(required=True, description='Feature',
                           example='Feature')],
    ['id', fields.Integer(required=True, description='Feature ID',
                          example=123)],
    ['geometry', fields.Nested(geojson_geometry, required=True,
                               description='Feature geometry')],
    ['properties', FeatureProperties(required=True,
                                     description='Feature properties',
                                     example={'name': 'Example', 'type': 2,
                                              'num': 4}
                                     )],
])

geojson_feature_collection_response = create_model(api, 'FeatureCollection', [
    ['type', fields.String(required=True, description='FeatureCollection',
                           example='FeatureCollection')],
    ['features', fields.List(fields.Nested(geojson_feature_member),
                             required=True, description='Features')],
    ['crs', fields.Nested(geojson_crs, required=True,
                          description='Coordinate reference system')]
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


# routes
@api.route('/<dataset>/')
@api.response(400, 'Bad request')
@api.response(404, 'Dataset not found or permission error')
@api.param('dataset', 'Dataset ID', default='qwc_demo.edit_points')
class DataCollection(Resource):
    @api.doc('index')
    @api.response(405, 'Dataset not readable')
    @api.param('bbox', 'Bounding box')
    @api.param('crs', 'Client coordinate reference system')
    @api.param('filter', 'Comma-separated list of filter expressions of the '
               'form "a = b" and "c like d"')
    @api.expect(index_parser)
    @api.marshal_with(geojson_feature_collection_response)
    @jwt_optional
    def get(self, dataset):
        """Get dataset features

        Return dataset features inside bounding box as a
        GeoJSON FeatureCollection.

        Query parameters:

        <b>bbox</b>: Bounding box as
                     <b>&lt;minx>,&lt;miny>,&lt;maxx>,&lt;maxy></b>,
                     e.g. <b>950700,6003900,950800,6004000</b>

        <b>crs</b>: Client CRS, e.g. <b>EPSG:3857<b>
        """
        args = index_parser.parse_args()
        bbox = args['bbox']
        crs = args['crs']
        filterexpr = args['filter']

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
        result = data_service.destroy(get_jwt_identity(), dataset, id)
        if 'error' not in result:
            return {
                'message': "Dataset feature deleted"
            }
        else:
            error_code = result.get('error_code') or 404
            api.abort(error_code, result['error'])


# local webserver
if __name__ == '__main__':
    print("Starting Data service...")
    app.run(host='localhost', port=5012, debug=True)
