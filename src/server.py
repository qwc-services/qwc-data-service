from collections import OrderedDict
import json
import os
import re
import requests

from flask import Flask, Request as RequestBase, request, jsonify, send_file
from flask_restx import Api, Resource, fields, reqparse, marshal
from werkzeug.exceptions import BadRequest
from werkzeug.datastructures import FileStorage

from qwc_services_core.api import create_model, CaseInsensitiveArgument
from qwc_services_core.auth import auth_manager, optional_auth, get_identity
from qwc_services_core.runtime_config import RuntimeConfig
from qwc_services_core.tenant_handler import (
    TenantHandler, TenantPrefixMiddleware, TenantSessionInterface)
from qwc_services_core.translator import Translator
from data_service import DataService


class Request(RequestBase):
    """Custom Flask Request subclass"""
    def on_json_loading_failed(self, e):
        """Always return detailed JSON decode error, not only in debug mode"""
        raise BadRequest('Failed to decode JSON object: {0}'.format(e))


# Flask application
app = Flask(__name__)
# use custom Request subclass
app.request_class = Request
# Flask-RESTPlus Api
api = Api(app, version='1.0', title='Data service API',
          description="""API for QWC Data service.""",
          default_label='Data edit operations', doc='/api/'
          )
# Omit X-Fields header in docs
app.config['RESTPLUS_MASK_SWAGGER'] = False
# disable verbose 404 error message
app.config['ERROR_404_HELP'] = False

auth = auth_manager(app, api)

# create tenant handler
tenant_handler = TenantHandler(app.logger)
app.wsgi_app = TenantPrefixMiddleware(app.wsgi_app)
app.session_interface = TenantSessionInterface()

def data_service_handler():
    """Get or create a DataService instance for a tenant."""
    tenant = tenant_handler.tenant()
    handler = tenant_handler.handler('data', 'data', tenant)
    if handler is None:
        config_handler = RuntimeConfig("data", app.logger)
        config = config_handler.tenant_config(tenant)
        handler = tenant_handler.register_handler(
            'data', tenant, DataService(tenant, app.logger, config))
    return handler


def verify_captcha(identity, captcha_response):
    """ Validate a captcha response."""
    # if authenticated, skip captcha validation
    if identity:
        return True
    tenant = tenant_handler.tenant()
    config_handler = RuntimeConfig("data", app.logger)
    config = config_handler.tenant_config(tenant)
    site_key = config.get("recaptcha_site_secret_key", "")
    if not site_key:
        app.logger.info(
            "recaptcha_site_secret_key is not set, skipping verification"
        )
        return True

    # send request to reCAPTCHA API
    app.logger.info("Verifying captcha response token")
    url = 'https://www.google.com/recaptcha/api/siteverify'
    params = {
        'secret': site_key,
        'response': captcha_response
    }
    response = requests.post(
        url, data=params, timeout=60
    )

    if response.status_code != requests.codes.ok:
        # handle server error
        app.logger.error(
            "Could not verify captcha response token:\n\n%s" %
            response.text
        )
        return False

    # check response
    res = json.loads(response.text)
    if res['success']:
        app.logger.info("Captcha verified")
        return True
    else:
        app.logger.warning("Captcha verification failed: %s" % res)

    return False


class FeatureId(fields.Raw):
    def format(self, value):
        if isinstance(value, int):
            return value
        else:
            return str(value)

class KeepEmptyDict(fields.Nested):
    def output(self, key, obj, **kwargs):
        if not obj.get(key):
            return {}
        return super().output(key, obj, **kwargs)

# Api models
extent_response = create_model(api, 'BBOX', [
    ['bbox', fields.Raw(required=True, allow_null=True,
                        description=(
                            'Extent of feature as [minx, miny, maxx, maxy]'
                        ),
                        example=[950598.0, 6003950.0, 950758.0, 6004010.0])]
])

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

# Feature
geojson_feature = create_model(api, 'Feature', [
    ['type', fields.String(required=True, description='Feature',
                           example='Feature')],
    ['id', FeatureId(required=True, description='Feature ID',
                          example=123)],
    ['geometry', fields.Nested(geojson_geometry, required=False,
                               allow_null=True,
                               description='Feature geometry')],
    ['properties', fields.Raw(required=True,
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

geojson_feature_request = api.inherit('Relation Feature', geojson_feature, {
    'defaultedProperties': fields.List(fields.String, required=False)
})

geojson_feature_collection = create_model(api, 'FeatureCollection', [
    ['type', fields.String(required=True, description='FeatureCollection',
                           example='FeatureCollection')],
    ['features', fields.List(fields.Nested(geojson_feature),
                             required=True, description='Features')],
    ['crs', fields.Nested(geojson_crs, required=False, allow_null=True,
                          description='Coordinate reference system')],
    ['bbox', fields.Raw(required=False, allow_null=True,
                        description=(
                            'Extent of features as [minx, miny, maxx, maxy]'
                        ),
                        example=[950598.0, 6003950.0, 950758.0, 6004010.0])]
])

# Relations
relation_feature = api.inherit('Relation Feature', geojson_feature, {
    '__status__': fields.String(required=False, description='Feature status'),
    'error': fields.String(required=False, description='Commit error'),
    'error_details': fields.Raw(required=False, description='Commit error details properties')
})

relation_table_features = create_model(api, 'Relation table features', [
    ['fk', fields.String(required=True, description='Foreign key field name')],
    ['features', fields.List(fields.Nested(relation_feature), required=True, description='Relation features')],
    ['error', fields.Raw(required=False, description='Error details')]
])

relation_values = create_model(api, 'Relation values', [
    ['*', fields.Wildcard(fields.Nested(relation_table_features, required=False, description='Relation table features'))]
])

geojson_feature_with_relvals = api.inherit('Feature with relation values', geojson_feature, {
    'relationValues': KeepEmptyDict(relation_values,
                                     required=False,
                                     description='Relation table entry')
})

# keyvals response
keyval_records = create_model(api, 'Keyval table record', [
    ['key', fields.Raw(required=True, description='Key')],
    ['value', fields.String(required=True, description='Value')]
])

keyvals_table_entry = create_model(api, 'Keyvals table entry', [
    ['*',  fields.Wildcard(fields.List(fields.Nested(keyval_records),
                      required=False, description='Keyval records'))]
])

keyvals_response = create_model(api, 'Keyval relation values', [
    ['keyvalues', fields.Nested(keyvals_table_entry,
                                     required=True,
                                     description='Keyval table entry')]
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
index_parser.add_argument('filter_geom')

feature_multipart_parser = reqparse.RequestParser(argument_class=CaseInsensitiveArgument)
feature_multipart_parser.add_argument('feature', help='Feature', required=True, location='form')
feature_multipart_parser.add_argument('file_document', help='File attachments', type=FileStorage, location='files')
feature_multipart_parser.add_argument('g-recaptcha-response', help="Recaptcha response", location='form')

show_parser = reqparse.RequestParser(argument_class=CaseInsensitiveArgument)
show_parser.add_argument('crs')

# attachment
get_attachment_parser = reqparse.RequestParser(argument_class=CaseInsensitiveArgument)
get_attachment_parser.add_argument('file', required=True)

# Relations
get_relations_parser = reqparse.RequestParser(argument_class=CaseInsensitiveArgument)
get_relations_parser.add_argument('tables', required=True)
get_relations_parser.add_argument('crs')
get_relations_parser.add_argument('filter')

# routes
@api.route('/<path:dataset>/')
@api.response(400, 'Bad request')
@api.response(404, 'Dataset not found or permission error')
@api.param('dataset', 'Dataset ID')
class FeatureCollection(Resource):
    @api.doc('index')
    @api.response(405, 'Dataset not readable')
    @api.param('bbox', 'Bounding box as `<minx>,<miny>,<maxx>,<maxy>`')
    @api.param('crs', 'Client coordinate reference system, e.g. `EPSG:3857`')
    @api.param(
        'filter', 'JSON serialized array of filter expressions: '
        '`[["<name>", "<op>", <value>],"and|or",["<name>","<op>",<value>]]`')
    @api.param(
        'filter_geom', 'GeoJSON serialized geometry, used as intersection geometry filter')
    @api.expect(index_parser)
    @api.marshal_with(geojson_feature_collection, skip_none=True)
    @optional_auth
    def get(self, dataset):
        """Get dataset features

        Return dataset features inside bounding box and matching filter as a
        GeoJSON FeatureCollection.
        """
        translator = Translator(app, request)
        args = index_parser.parse_args()
        bbox = args['bbox']
        crs = args['crs']
        filterexpr = args['filter']
        filter_geom = args['filter_geom']

        data_service = data_service_handler()
        result = data_service.index(
            get_identity(), translator, dataset, bbox, crs, filterexpr, filter_geom
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
    @api.marshal_with(geojson_feature, code=201)
    @optional_auth
    def post(self, dataset):
        """Create a new dataset feature

        Create new dataset feature from a GeoJSON Feature and return it as a
        GeoJSON Feature.
        """
        translator = Translator(app, request)

        if request.is_json:
            # parse request data (NOTE: catches invalid JSON)
            feature = api.payload
            if isinstance(feature, dict):
                data_service = data_service_handler()

                result = data_service.create(
                    get_identity(), translator, dataset, feature)
                if 'error' not in result:
                    return result['feature'], 201
                else:
                    error_code = result.get('error_code') or 404
                    error_details = result.get('error_details') or {}
                    api.abort(error_code, result['error'], **error_details)
            else:
                api.abort(400, translator.tr("error.json_is_not_an_object"))
        else:
            api.abort(400, translator.tr("error.request_data_is_not_json"))


@api.route('/<path:dataset>/extent')
@api.response(400, 'Bad request')
@api.response(404, 'Dataset not found or permission error')
@api.param('dataset', 'Dataset ID')
class FeatureCollectionExtent(Resource):
    @api.doc('index')
    @api.response(405, 'Dataset not readable')
    @api.param('crs', 'Client coordinate reference system, e.g. `EPSG:3857`')
    @api.param(
        'filter', 'JSON serialized array of filter expressions: '
        '`[["<name>", "<op>", <value>],"and|or",["<name>","<op>",<value>]]`')
    @api.param(
        'filter_geom', 'GeoJSON serialized geometry, used as intersection geometry filter')
    @api.expect(index_parser)
    @api.marshal_with(extent_response)
    @optional_auth
    def get(self, dataset):
        """Get dataset features

        Return the extend of the features matching any specified filter as a
        [xmin,ymin,xmax,ymax] array.
        """
        translator = Translator(app, request)
        args = index_parser.parse_args()
        crs = args['crs']
        filterexpr = args['filter']
        filter_geom = args['filter_geom']

        data_service = data_service_handler()
        result = data_service.extent(
            get_identity(), translator, dataset, crs, filterexpr, filter_geom
        )
        if 'error' not in result:
            return result['extent']
        else:
            error_code = result.get('error_code') or 404
            api.abort(error_code, result['error'])


@api.route('/<path:dataset>/<id>')
@api.response(404, 'Dataset or feature not found or permission error')
@api.param('dataset', 'Dataset ID')
@api.param('id', 'Feature ID')
class Feature(Resource):
    @api.doc('show')
    @api.response(405, 'Dataset not readable')
    @api.param('crs', 'Client coordinate reference system')
    @api.expect(show_parser)
    @api.marshal_with(geojson_feature)
    @optional_auth
    def get(self, dataset, id):
        """Get a dataset feature

        Return dataset feature with ID as a GeoJSON Feature.

        Query parameter:

        <b>crs</b>: Client CRS, e.g. <b>EPSG:3857<b>
        """
        translator = Translator(app, request)
        args = show_parser.parse_args()
        crs = args['crs']

        data_service = data_service_handler()
        result = data_service.show(get_identity(), translator, dataset, id, crs)
        if 'error' not in result:
            return result['feature']
        else:
            api.abort(404, result['error'])

    @api.doc('update')
    @api.response(400, 'Bad request')
    @api.response(405, 'Dataset not updatable')
    @api.response(422, 'Feature validation failed', feature_validation_response)
    @api.expect(geojson_feature_request)
    @api.marshal_with(geojson_feature)
    @optional_auth
    def put(self, dataset, id):
        """Update a dataset feature

        Update dataset feature with ID from a GeoJSON Feature and return it as
        a GeoJSON Feature.
        """
        translator = Translator(app, request)
        if request.is_json:
            # parse request data (NOTE: catches invalid JSON)
            feature = api.payload
            if isinstance(feature, dict):
                data_service = data_service_handler()

                result = data_service.update(
                    get_identity(), translator, dataset, id, feature
                )
                if 'error' not in result:
                    return result['feature']
                else:
                    error_code = result.get('error_code') or 404
                    error_details = result.get('error_details') or {}
                    api.abort(error_code, result['error'], **error_details)
            else:
                api.abort(400, translator.tr("error.json_is_not_an_object"))
        else:
            api.abort(400, translator.tr("error.request_data_is_not_json"))

    @api.doc('destroy')
    @api.response(405, 'Dataset not deletable')
    @api.marshal_with(message_response)
    @optional_auth
    def delete(self, dataset, id):
        """Delete a dataset feature

        Delete dataset feature with ID.
        """

        translator = Translator(app, request)

        captcha_response = api.payload.get('g-recaptcha-response') if request.is_json else None
        if not verify_captcha(get_identity(), captcha_response):
            api.abort(400, translator.tr("error.captcha_validation_failed"))

        data_service = data_service_handler()
        result = data_service.destroy(get_identity(), translator, dataset, id)
        if 'error' not in result:
            return {
                'message': translator.tr("error.dataset_feature_deleted")
            }
        else:
            error_code = result.get('error_code') or 404
            api.abort(error_code, result['error'])


@api.route('/<path:dataset>/multipart')
@api.response(400, 'Bad request')
@api.response(404, 'Dataset not found or permission error')
@api.param('dataset', 'Dataset ID')
class CreateFeatureMultipart(Resource):
    @api.doc('create')
    @api.response(405, 'Dataset not creatable')
    @api.response(422, 'Feature validation failed', feature_validation_response)
    @api.expect(feature_multipart_parser)
    @api.marshal_with(geojson_feature_with_relvals, code=201)
    @optional_auth
    def post(self, dataset):
        """Create a new dataset feature

        Create new dataset feature from a GeoJSON Feature and return it as a
        GeoJSON Feature.
        """
        translator = Translator(app, request)
        args = feature_multipart_parser.parse_args()

        if not verify_captcha(get_identity(), args['g-recaptcha-response']):
            api.abort(400, translator.tr("error.captcha_validation_failed"))

        try:
            feature = json.loads(args['feature'])
        except:
            feature = None
        if not isinstance(feature, dict):
            api.abort(400, translator.tr("error.feature_is_not_an_object"))

        files = dict([entry for entry in request.files.items() if entry[0].startswith("file:")])
        # Set a placeholder value to make attribute validation for required upload fields pass
        for key in request.files:
            parts = key.split(":")
            if parts[0] == 'file':
                field = parts[1]
                feature['properties'][field] = request.files[key].filename

        data_service = data_service_handler()
        result = data_service.create(
            get_identity(), translator, dataset, feature, files
        )
        if 'error' not in result:
            relationValues = data_service.write_relation_values(get_identity(), result['feature']['id'], feature.get('relationValues', '{}'), request.files, translator, True)
            # Requery feature because the write_relation_values may change the feature through DB triggers
            crs = feature['crs']['properties']['name'] if feature['crs'] else None
            result = data_service.show(get_identity(), translator, dataset, result['feature']['id'], crs)
            if 'error' not in result:
                feature = result['feature']
                feature['relationValues'] = relationValues
                return feature, 201

        error_code = result.get('error_code') or 404
        error_details = result.get('error_details') or {}
        api.abort(error_code, result['error'], **error_details)


@api.route('/<path:dataset>/multipart/<id>')
@api.response(404, 'Dataset or feature not found or permission error')
@api.param('dataset', 'Dataset ID')
@api.param('id', 'Feature ID')
class EditFeatureMultipart(Resource):
    @api.doc('update')
    @api.response(400, 'Bad request')
    @api.response(405, 'Dataset not updatable')
    @api.response(422, 'Feature validation failed', feature_validation_response)
    @api.expect(feature_multipart_parser)
    @api.marshal_with(geojson_feature_with_relvals)
    @optional_auth
    def put(self, dataset, id):
        """Update a dataset feature

        Update dataset feature with ID from a GeoJSON Feature and return it as
        a GeoJSON Feature.
        """
        translator = Translator(app, request)
        args = feature_multipart_parser.parse_args()

        if not verify_captcha(get_identity(), args['g-recaptcha-response']):
            api.abort(400, translator.tr("error.captcha_validation_failed"))

        try:
            feature = json.loads(args['feature'])
        except:
            feature = None
        if not isinstance(feature, dict):
            api.abort(400, translator.tr("error.feature_is_not_an_object"))

        files = dict([entry for entry in request.files.items() if entry[0].startswith("file:")])

        data_service = data_service_handler()
        result = data_service.update(
            get_identity(), translator, dataset, id, feature, files
        )
        if 'error' not in result:
            relationValues = data_service.write_relation_values(get_identity(), result['feature']['id'], feature.get('relationValues', {}), request.files, translator)
            # Requery feature because the write_relation_values may change the feature through DB triggers
            crs = feature['crs']['properties']['name'] if feature['crs'] else None
            result = data_service.show(get_identity(), translator, dataset, id, crs)
            if 'error' not in result:
                feature = result['feature']
                feature['relationValues'] = relationValues
                return feature

        error_code = result.get('error_code') or 404
        error_details = result.get('error_details') or {}
        api.abort(error_code, result['error'], **error_details)


@api.route('/<path:dataset>/attachment')
@api.response(404, 'Dataset or feature not found or permission error')
@api.param('dataset', 'Dataset ID')
class AttachmentDownloader(Resource):
    @api.doc('get_attachment')
    @api.param('file', 'The file to download')
    @api.expect(get_attachment_parser)
    @optional_auth
    def get(self, dataset):
        translator = Translator(app, request)
        args = get_attachment_parser.parse_args()
        data_service = data_service_handler()
        result = data_service.resolve_attachment(get_identity(), translator, dataset, args['file'])
        if 'error' not in result:
            return send_file(result['file'], as_attachment=True, download_name=os.path.basename(result['file']))
        else:
            error_code = result.get('error_code') or 404
            error_details = result.get('error_details') or {}
            api.abort(error_code, result['error'], **error_details)


@api.route('/<path:dataset>/<id>/relations')
@api.response(404, 'Dataset or feature not found or permission error')
@api.param('dataset', 'Dataset ID')
@api.param('id', 'Feature ID')
class Relations(Resource):
    @api.doc('get_relations')
    @api.param('tables', 'Comma separated list of relation tables of the form "tablename:fk_field_name"')
    @api.expect(get_relations_parser)
    @api.marshal_with(relation_values, code=201)
    @optional_auth
    def get(self, dataset, id):
        translator = Translator(app, request)
        data_service = data_service_handler()
        args = get_relations_parser.parse_args()
        relations = args['tables'] or ""
        crs = args['crs'] or None
        ret = {}
        for relation in relations.split(","):
            try:
                table, fk_field_name, sortcol = (relation + ":").split(":")[0:3]
            except:
                continue
            result = data_service.index(
                get_identity(), translator, table, None, crs, '[["%s", "=", "%s"]]' % (fk_field_name, id), None
            )
            ret[table] = {
                "fk": fk_field_name,
                "features": result['feature_collection']['features'] if 'feature_collection' in result else [],
                "error": result.get('error')
            }
            if sortcol:
                ret[table]['features'].sort(key=lambda f: f["properties"][sortcol])
            else:
                ret[table]['features'].sort(key=lambda f: f["id"])
        return ret


@api.route('/keyvals')
@api.response(404, 'Dataset or feature not found or permission error')
class KeyValues(Resource):
    @api.doc('get_relations')
    @api.param('tables', 'Comma separated list of keyvalue tables of the form "tablename:key_field_name:value_field_name"')
    @api.param(
        'filter', 'JSON serialized array of filter expressions, the same length as the number of specified tables: '
        '`[[["<name>", "<op>", <value>],"and|or",["<name>","<op>",<value>]], ...]`')
    @api.expect(get_relations_parser)
    @api.marshal_with(keyvals_response, code=201)
    @optional_auth
    def get(self):
        translator = Translator(app, request)
        args = get_relations_parser.parse_args()
        filterexpr = json.loads(args.get('filter') or "[]")

        data_service = data_service_handler()

        keyvals = args['tables'] or ""
        ret = {}
        for (idx, keyval) in enumerate(keyvals.split(",")):
            try:
                table, key_field_name, value_field_name = keyval.split(":")
            except:
                continue
            ret[table] = []
            result = data_service.index(
                get_identity(), translator, table, None, None, json.dumps(filterexpr[idx]) if filterexpr and len(filterexpr) > idx and filterexpr[idx] else None, None
            )
            if 'feature_collection' in result:
                for feature in result['feature_collection']['features']:
                    record = {"key": feature["id"] if key_field_name == "id" else feature['properties'][key_field_name], "value": str(feature['properties'][value_field_name]).strip()}
                    ret[table].append(record)
                natsort = lambda s: [int(t) if t.isdigit() else t.lower() for t in re.split('(\d+)', s)]
                ret[table].sort(key=lambda record: natsort(record["value"]))
            elif 'error' in result:
                app.logger.debug(f"Failed to query relation values for {keyval}: {result['error']}")
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
