import unittest
from unittest.mock import patch

from flask import Response, json
from flask.testing import FlaskClient
from flask_jwt_extended import JWTManager, create_access_token

import server
from service_lib.fixtures import GeoFixtures, SharedConnection


class FeatureValidationTestCase(unittest.TestCase):
    """Test case for feature validations"""

    def setUp(self):
        server.app.testing = True
        self.app = FlaskClient(server.app, Response)
        JWTManager(server.app)
        self.fid = 1
        self.dataset = 'test_polygons'

    def tearDown(self):
        pass

    def jwtHeader(self):
        """Return JWT header for user 'demo'."""
        with server.app.test_request_context():
            access_token = create_access_token('test')
        return {'Authorization': 'Bearer {}'.format(access_token)}

    def send_request(self, method, url, data, as_json=True):
        """Send POST or PUT request with JSON data and return status code and
        decoded JSON from response.

        :param str method: Request method 'POST' or 'PUT'
        :param str url: Request URL
        :param str data: Raw request data
        :param bool as_json: Send as 'application/json' if set
        """
        self.assertIn(method, ['POST', 'PUT'])
        if method == "POST":
            request_method = self.app.post
        elif method == 'PUT':
            request_method = self.app.put

        if as_json:
            # send data as JSON
            response = request_method(url, data=data, headers=self.jwtHeader(),
                                      content_type='application/json')
        else:
            # send data as form
            response = request_method(url, data=data, headers=self.jwtHeader())
        return response.status_code, json.loads(response.data)

    def check_status_and_message(self, status_code, json_data, status, message):
        """Check expected status code and message.

        :param int status_code: Status code from response
        :param obj json_data: Response object
        :param int status: Expected status code
        :param str message: Expected starting text of response message
        """
        # check status code
        status_codes = {
            200: 'OK',
            201: 'Created',
            400: 'Bad Request',
            422: 'Unprocessable Entity'
        }
        self.assertEqual(status, status_code, "Status code is not %s" %
                         status_codes.get(status, status))

        if message is not None:
            # check error message
            self.assertRegex(json_data['message'], r'^%s' % message,
                             "Message does not match")

    # bad requests

    def check_responses(self, data, status, message, as_json=True):
        """Send 'create' and  'update' requests with raw data
        and check response status and message.

        :param str data: Raw request data
        :param int status: Expected status code
        :param str message: Expected starting text of response message
        :param bool as_json: Send as 'application/json' if set
        """
        # create
        url = "/%s/" % self.dataset
        status_code, json_data = self.send_request('POST', url, data, as_json)
        self.check_status_and_message(status_code, json_data, status, message)

        # update
        url = "/%s/%d" % (self.dataset, self.fid)
        status_code, json_data = self.send_request('PUT', url, data, as_json)
        self.check_status_and_message(status_code, json_data, status, message)

    def test_no_raw_data(self):
        self.check_responses(None, 400, 'Request data is not JSON', False)

    def test_raw_data(self):
        self.check_responses('test', 400, 'Request data is not JSON', False)

    def test_json_string_raw_data(self):
        data = '{"test":123}'
        self.check_responses(data, 400, 'Request data is not JSON', False)

    def test_no_json_data(self):
        self.check_responses(None, 400, 'Failed to decode JSON object: ')

    def test_empty_json(self):
        self.check_responses('', 400, 'Failed to decode JSON object: ')

    def test_invalid_json(self):
        # string
        self.check_responses('test', 400, 'Failed to decode JSON object: ')

        # missing quotes
        data = '{test: 123}'
        self.check_responses(data, 400, 'Failed to decode JSON object: ')

        # fragment
        data = '{"test":123,"test2":"value'
        self.check_responses(data, 400, 'Failed to decode JSON object: ')

    def test_non_object_json(self):
        # quoted string
        self.check_responses('"test"', 400, 'JSON is not an object')

        # number
        self.check_responses('123', 400, 'JSON is not an object')

    # invalid GeoJSON

    def check_validation_errors(self, json_data, validation_errors,
                                geometry_errors):
        """Check expected validation and geometry errors.

        :param obj json_data: Response object
        :param list[str] validation_errors: Expected validation errors
        :param list[obj] geometry_errors: Expected geometry errors
        """
        if validation_errors is not None:
            self.assertIn('validation_errors', json_data,
                          "Missing validation errors")
            self.assertCountEqual(validation_errors,
                                  json_data['validation_errors'],
                                  "Validation errors do not match")
        else:
            self.assertNotIn('validation_errors', json_data,
                             "Unexpected validation errors")

        if geometry_errors is not None:
            self.assertIn('geometry_errors', json_data,
                          "Missing geometry errors")
            self.assertCountEqual(geometry_errors,
                                  json_data['geometry_errors'],
                                  "Geometry errors do not match")
        else:
            self.assertNotIn('geometry_errors', json_data,
                             "Unexpected geometry errors")

    def check_validation_responses(self, input_feature, validation_errors,
                                   geometry_errors, skip_update=False):
        """Send 'create' and 'update' requests with invalid GeoJSON
        and check response status and contents.

        :param obj input_feature: Feature object
        :param list[str] validation_errors: Expected validation errors
        :param list[obj] geometry_errors: Expected geometry errors
        :param bool skip_update: Set to skip update request (default: False)
        """
        # create JSON string
        data = json.dumps(input_feature)

        # create
        url = "/%s/" % self.dataset
        if validation_errors is None and geometry_errors is None:
            status = 201
            message = None
        else:
            status = 422
            message = 'Feature validation failed'
        status_code, json_data = self.send_request('POST', url, data)
        self.check_status_and_message(status_code, json_data, status, message)
        self.check_validation_errors(json_data, validation_errors,
                                     geometry_errors)

        if skip_update:
            # skip update request to avoid followup transaction error
            # after e.g. 'Too few ordinates in GeoJSON'
            return

        # update
        url = "/%s/%d" % (self.dataset, self.fid)
        if validation_errors is None and geometry_errors is None:
            status = 200
        status_code, json_data = self.send_request('PUT', url, data)
        self.check_status_and_message(status_code, json_data, status, message)
        self.check_validation_errors(json_data, validation_errors,
                                     geometry_errors)

    def build_feature(self):
        """Create GeoJSON feature"""
        return {
            'type': 'Feature',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[[2606900, 1228600], [2606910, 1228600], [2606910, 1228610], [2606900, 1228600]]]
            },
            'properties': {
                'name': 'Test',
                'beschreibung': 'Test Polygon'
            },
            'crs': {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::2056'
                }
            }
        }

    def test_empty_object(self):
        input_feature = {}
        validation_errors = [
            'GeoJSON must be of type Feature',
            'Missing GeoJSON properties',
            'Missing GeoJSON CRS'
        ]
        self.check_validation_responses(input_feature, validation_errors, None)

    def test_non_geojson_object(self):
        input_feature = {
            'test': 123,
            'type': [1, 2, 3]
        }
        validation_errors = [
            'GeoJSON must be of type Feature',
            'Missing GeoJSON properties',
            'Missing GeoJSON CRS'
        ]
        self.check_validation_responses(input_feature, validation_errors, None)

    def test_invalid_type(self):
        input_feature = self.build_feature()
        validation_errors = [
            'GeoJSON must be of type Feature'
        ]

        # invalid type
        input_feature['type'] = 'Test'
        self.check_validation_responses(input_feature, validation_errors, None)

        # no type
        input_feature['type'] = None
        self.check_validation_responses(input_feature, validation_errors, None)

        # missing type
        del input_feature['type']
        self.check_validation_responses(input_feature, validation_errors, None)

    def test_invalid_geometry_object(self):
        input_feature = self.build_feature()
        validation_errors = [
            'Invalid GeoJSON geometry type'
        ]

        # invalid geometry type
        input_feature['geometry']['type'] = 'test'
        self.check_validation_responses(input_feature, validation_errors, None)

        # no geometry type
        input_feature['geometry']['type'] = None
        self.check_validation_responses(input_feature, validation_errors, None)

        # missing geometry type
        validation_errors = [
            'Missing GeoJSON geometry type'
        ]
        del input_feature['geometry']['type']
        self.check_validation_responses(input_feature, validation_errors, None)

        input_feature = self.build_feature()
        validation_errors = [
            'Invalid GeoJSON geometry coordinates'
        ]

        # invalid geometry coordinates
        input_feature['geometry']['coordinates'] = 'test'
        self.check_validation_responses(input_feature, validation_errors, None)

        input_feature['geometry']['coordinates'] = {}
        self.check_validation_responses(input_feature, validation_errors, None)

        # no geometry coordinates
        input_feature['geometry']['coordinates'] = None
        self.check_validation_responses(input_feature, validation_errors, None)

        validation_errors = [
            'Missing GeoJSON geometry coordinates'
        ]

        # missing geometry coordinates
        del input_feature['geometry']['coordinates']
        self.check_validation_responses(input_feature, validation_errors, None)

    def test_invalid_properties(self):
        input_feature = self.build_feature()
        validation_errors = [
            'Invalid GeoJSON properties'
        ]

        # invalid properties
        input_feature['properties'] = 'test'
        self.check_validation_responses(input_feature, validation_errors, None)

        # no properties
        input_feature['properties'] = None
        self.check_validation_responses(input_feature, validation_errors, None)

        validation_errors = [
            'Missing GeoJSON properties'
        ]

        # missing properties
        del input_feature['properties']
        self.check_validation_responses(input_feature, validation_errors, None)

    def test_empty_properties(self):
        input_feature = self.build_feature()
        input_feature['properties'] = {}
        self.check_validation_responses(input_feature, None, None)

    def test_unknown_or_forbidden_attributes(self):
        input_feature = self.build_feature()
        validation_errors = [
            "Feature property 'test' can not be set"
        ]

        # additional attribute
        input_feature['properties']['test'] = 123
        self.check_validation_responses(input_feature, validation_errors, None)

        # unknown attribute only
        input_feature['properties'] = {'test': 123}
        self.check_validation_responses(input_feature, validation_errors, None)

        input_feature = self.build_feature()
        validation_errors = [
            "Feature property 'test' can not be set",
            "Feature property 'test2' can not be set"
        ]

        # multiple additional attributes
        input_feature['properties']['test'] = 123
        input_feature['properties']['test2'] = 'test'
        self.check_validation_responses(input_feature, validation_errors, None)

    def test_invalid_attribute_types(self):
        input_feature = self.build_feature()
        validation_errors = [
            "Invalid type for feature property 'name'"
        ]

        # dict
        input_feature['properties']['name'] = {'test': 123}
        self.check_validation_responses(input_feature, validation_errors, None)

        # list
        input_feature['properties']['name'] = [1, 2, 3]
        self.check_validation_responses(input_feature, validation_errors, None)

        validation_errors = [
            "Invalid type for feature property 'name'",
            "Invalid type for feature property 'beschreibung'"
        ]

        # multiple
        input_feature['properties']['name'] = {}
        input_feature['properties']['beschreibung'] = []
        self.check_validation_responses(input_feature, validation_errors, None)

    def test_valid_attribute_types(self):
        input_feature = self.build_feature()

        # no value
        input_feature['properties']['name'] = None
        self.check_validation_responses(input_feature, None, None)

        # string
        input_feature['properties']['name'] = 'Test'
        self.check_validation_responses(input_feature, None, None)

        # int
        input_feature['properties']['name'] = 123
        self.check_validation_responses(input_feature, None, None)

        # float
        input_feature['properties']['name'] = 123.456
        self.check_validation_responses(input_feature, None, None)

    def test_invalid_crs(self):
        input_feature = self.build_feature()
        validation_errors = [
            "GeoJSON CRS must be of type 'name'"
        ]

        # invalid CRS type
        input_feature['crs']['type'] = 'test'
        self.check_validation_responses(input_feature, validation_errors, None)

        # no CRS type
        input_feature['crs']['type'] = None
        self.check_validation_responses(input_feature, validation_errors, None)

        # missing CRS type
        del input_feature['crs']['type']
        self.check_validation_responses(input_feature, validation_errors, None)

        input_feature = self.build_feature()
        validation_errors = [
            "GeoJSON CRS is not an OGC CRS URN "
            "(e.g. 'urn:ogc:def:crs:EPSG::4326')"
        ]

        # invalid CRS name
        input_feature['crs']['properties']['name'] = 'test'
        self.check_validation_responses(input_feature, validation_errors, None)

        # empty CRS properties
        input_feature['crs']['properties'] = {}
        self.check_validation_responses(input_feature, validation_errors, None)

        validation_errors = [
            'Invalid GeoJSON CRS properties'
        ]

        # invalid CRS properties
        input_feature['crs']['properties'] = 'test'
        self.check_validation_responses(input_feature, validation_errors, None)

        # no CRS properties
        input_feature['crs']['properties'] = None
        self.check_validation_responses(input_feature, validation_errors, None)

        validation_errors = [
            'Missing GeoJSON CRS properties'
        ]

        # missing CRS properties
        del input_feature['crs']['properties']
        self.check_validation_responses(input_feature, validation_errors, None)

    # invalid geometry

    def test_empty_geometry(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {'reason': 'Empty or incomplete geometry'}
        ]

        # linestring
        input_feature['geometry']['type'] = 'LineString'
        input_feature['geometry']['coordinates'] = []
        self.check_validation_responses(input_feature, None, geometry_errors)

        # polygon
        input_feature['geometry']['type'] = 'Polygon'
        input_feature['geometry']['coordinates'] = []
        self.check_validation_responses(input_feature, None, geometry_errors)

        input_feature['geometry']['coordinates'] = [[2606900, 1228600]]
        self.check_validation_responses(input_feature, None, geometry_errors)

    def test_incomplete_geometry(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {'reason': ('IllegalArgumentException: Points of LinearRing '
                        'do not form a closed linestring')}
        ]

        # not enough vertices
        input_feature['geometry']['type'] = 'Polygon'
        input_feature['geometry']['coordinates'] = [[
            [2606900, 1228600], [2606950, 1228600]
        ]]
        self.check_validation_responses(input_feature, None, geometry_errors)

        # last vertex missing
        input_feature['geometry']['coordinates'] = [[
            [2606900, 1228600], [2606950, 1228600], [2606950, 1228650],
            [2606900, 1228650]
        ]]
        self.check_validation_responses(input_feature, None, geometry_errors)

    def test_missing_coords(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {'reason': 'Empty or incomplete geometry'}
        ]

        # empty point
        input_feature['geometry']['coordinates'] = []
        self.check_validation_responses(input_feature, None, geometry_errors, True)

    def test_missing_coords_missing_coord(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {'reason': 'Too few ordinates in GeoJSON'}
        ]

        # missing coordinate
        input_feature['geometry']['type'] = 'Polygon'
        input_feature['geometry']['coordinates'] = [[
            [2606900, 1228600], [1228600], [2606950, 1228650],
            [2606900, 1228650], [2606900, 1228600]
        ]]
        self.check_validation_responses(input_feature, None, geometry_errors, True)

    def test_missing_coords_missing_pair(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {'reason': 'Too few ordinates in GeoJSON'}
        ]

        # missing coordinate pair
        input_feature['geometry']['coordinates'] = [[
            [2606900, 1228600], [2606950, 1228600], [],
            [2606900, 1228650], [2606900, 1228600]
        ]]
        self.check_validation_responses(input_feature, None, geometry_errors, True)

    def test_self_intersection(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {
                'reason': 'Self-intersection',
                'location': 'POINT(2606925 1228625)'
            }
        ]

        input_feature['geometry']['type'] = 'Polygon'
        input_feature['geometry']['coordinates'] = [[
            [2606900, 1228600], [2606950, 1228600], [2606900, 1228650],
            [2606950, 1228650], [2606900, 1228600]
        ]]
        self.check_validation_responses(input_feature, None, geometry_errors)

    def test_duplicate_point(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {
                'reason': 'Duplicated point',
                'location': 'POINT(2606950 1228600)'
            }
        ]

        # single duplicate
        input_feature['geometry']['type'] = 'Polygon'
        input_feature['geometry']['coordinates'] = [[
            [2606900, 1228600], [2606950, 1228600], [2606950, 1228600],
            [2606950, 1228650], [2606900, 1228650], [2606900, 1228600]
        ]]
        self.check_validation_responses(input_feature, None, geometry_errors)

    def test_duplicate_points(self):
        input_feature = self.build_feature()
        geometry_errors = [
            {
                'reason': 'Duplicated point',
                'location': 'POINT(2606900 1228600)'
            },
            {
                'reason': 'Duplicated point',
                'location': 'POINT(2606950 1228650)'
            },
            {
                'reason': 'Duplicated point',
                'location': 'POINT(2606950 1228650)'
            }
        ]

        # multiple duplicates
        input_feature['geometry']['type'] = 'Polygon'
        input_feature['geometry']['coordinates'] = [[
            [2606900, 1228600], [2606900, 1228600], [2606950, 1228600],
            [2606950, 1228650], [2606950, 1228650], [2606950, 1228650],
            [2606900, 1228650], [2606900, 1228600]
        ]]
        self.check_validation_responses(input_feature, None, geometry_errors)

    def test_invalid_geometry_type(self):
        input_feature = self.build_feature()

        # linestring
        geometry_errors = [
            {'reason': 'Invalid geometry type: LINESTRING is not a POLYGON'}
        ]
        input_feature['geometry']['type'] = 'LineString'
        input_feature['geometry']['coordinates'] = [
            [2606900, 1228600], [2606950, 1228600], [2606950, 1228650],
        ]
        self.check_validation_responses(input_feature, None, geometry_errors)

        # polygon
        geometry_errors = [
            {'reason': 'Invalid geometry type: POINT is not a POLYGON'}
        ]
        input_feature['geometry']['type'] = 'Point'
        input_feature['geometry']['coordinates'] = [2606950, 1228650]
        self.check_validation_responses(input_feature, None, geometry_errors)
