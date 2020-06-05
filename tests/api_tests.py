import unittest
from unittest.mock import patch

from flask import Response, json
from flask.testing import FlaskClient
from flask_jwt_extended import JWTManager, create_access_token

import server


class ApiTestCase(unittest.TestCase):
    """Test case for server API"""

    def setUp(self):
        server.app.testing = True
        self.app = FlaskClient(server.app, Response)
        JWTManager(server.app)
        self.dataset = 'test_polygons'
        self.dataset_read_only = 'test_points'

    def tearDown(self):
        pass

    def jwtHeader(self):
        with server.app.test_request_context():
            access_token = create_access_token('test')
        return {'Authorization': 'Bearer {}'.format(access_token)}

    def get(self, url):
        """Send GET request and return status code and decoded JSON from
        response.
        """
        response = self.app.get(url, headers=self.jwtHeader())
        return response.status_code, json.loads(response.data)

    def post(self, url, json_data):
        """Send POST request with JSON data and return status code and
        decoded JSON from response.
        """
        data = json.dumps(json_data)
        response = self.app.post(url, data=data, headers=self.jwtHeader(),
                                 content_type='application/json')
        return response.status_code, json.loads(response.data)

    def put(self, url, json_data):
        """Send PUT request with JSON data and return status code and
        decoded JSON from response.
        """
        data = json.dumps(json_data)
        response = self.app.put(url, data=data, headers=self.jwtHeader(),
                                content_type='application/json')
        return response.status_code, json.loads(response.data)

    def delete(self, url):
        """Send DELETE request and return status code and decoded JSON from
        response.
        """
        response = self.app.delete(url, headers=self.jwtHeader())
        return response.status_code, json.loads(response.data)

    def check_feature(self, feature, has_crs=True):
        """Check GeoJSON feature."""
        self.assertEqual('Feature', feature['type'])
        self.assertIn('id', feature)
        self.assertIsInstance(feature['id'], int,
                              "Feature ID is not an integer")

        # check geometry
        self.assertIn('geometry', feature)
        geometry = feature['geometry']
        self.assertIn('type', geometry)
        geo_json_geometry_types = [
            'Point',
            'MultiPoint',
            'LineString',
            'MultiLineString',
            'Polygon',
            'MultiPolygon',
            'GeometryCollection'
        ]
        self.assertIn(geometry['type'], geo_json_geometry_types,
                      "Invalid GeoJSON geometry type")
        self.assertIn('coordinates', geometry)
        self.assertIsInstance(geometry['coordinates'], list,
                              "Feature geometry coordinates are not a list")

        # check properties
        self.assertIn('properties', feature)
        self.assertIsInstance(feature['properties'], dict,
                              "Feature properties are not a dict")

        # check CRS
        if has_crs:
            crs = {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::2056'
                }
            }
            self.assertEqual(crs, feature['crs'])
        else:
            self.assertNotIn('crs', feature)

        # check for surplus properties
        geo_json_feature_keys = ['type', 'id', 'geometry', 'properties', 'crs']
        for key in feature.keys():
            self.assertIn(key, geo_json_feature_keys,
                          "Invalid property for GeoJSON Feature")

    def build_poly_feature(self):
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

    def build_point_feature(self):
        """Create GeoJSON feature"""
        return {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [2606900, 1228600]
            },
            'properties': {
                'name': 'Test',
                'beschreibung': 'Test Punkt'
            },
            'crs': {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::2056'
                }
            }
        }

    # index

    def test_index(self):
        # without bbox
        status_code, json_data = self.get("/%s/" % self.dataset)
        self.assertEqual(200, status_code, "Status code is not OK")
        self.assertEqual('FeatureCollection', json_data['type'])
        assert len(json_data['features']) > 0, \
            "No Features in FeatureCollection"
        for feature in json_data['features']:
            self.check_feature(feature, False)
        crs = {
            'type': 'name',
            'properties': {
                'name': 'urn:ogc:def:crs:EPSG::2056'
            }
        }
        self.assertEqual(crs, json_data['crs'])
        no_bbox_count = len(json_data['features'])

        # with bbox
        bbox = '1288647,-4658384,1501913,-4538362'
        status_code, json_data = self.get("/%s/?bbox=%s" % (self.dataset, bbox))
        self.assertEqual(200, status_code, "Status code is not OK")
        self.assertEqual('FeatureCollection', json_data['type'])
        assert len(json_data['features']) > 0, \
            "No Features in FeatureCollection"
        for feature in json_data['features']:
            self.check_feature(feature, False)
        crs = {
            'type': 'name',
            'properties': {
                'name': 'urn:ogc:def:crs:EPSG::2056'
            }
        }
        self.assertEqual(crs, json_data['crs'])
        self.assertGreaterEqual(no_bbox_count, len(json_data['features']),
                           "Too many features within bbox.")

    def test_index_read_only(self):
        bbox = '1358925,-4604991,1431179,-4569265'
        status_code, json_data = self.get("/%s/?bbox=%s" %
                                          (self.dataset_read_only, bbox))
        self.assertEqual(200, status_code, "Status code is not OK")
        self.assertEqual('FeatureCollection', json_data['type'])
        assert len(json_data['features']) > 0, \
            "No Features in FeatureCollection"
        for feature in json_data['features']:
            self.check_feature(feature, False)
        crs = {
            'type': 'name',
            'properties': {
                'name': 'urn:ogc:def:crs:EPSG::2056'
            }
        }
        self.assertEqual(crs, json_data['crs'])

    def test_index_invalid_dataset(self):
        status_code, json_data = self.get('/invalid_dataset/')
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Dataset not found or permission error', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_index_empty_bbox(self):
        status_code, json_data = self.get("/%s/?bbox=" % self.dataset)
        self.assertEqual(400, status_code, "Status code is not Bad Request")
        self.assertEqual('Invalid bounding box', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_index_invalid_bbox(self):
        test_bboxes = [
            'test',  # string
            '123',  # number
            '2606900,1228600,2606925',  # not enough values
            '2606900,1228600,2606925,1228625,1234',  # too many values
            '2606900,test,2606925,1228625',  # invalid values
            '2606900,1228600,2606800,1228625',  # minx > maxx
            '2606900,1228600,2606925,1228500',  # miny > maxy
        ]

        for bbox in test_bboxes:
            status_code, json_data = self.get("/%s/?bbox=%s" %
                                              (self.dataset, bbox))
            self.assertEqual(400, status_code, "Status code is not Bad Request")
            self.assertEqual('Invalid bounding box', json_data['message'],
                             "Message does not match (bbox='%s')" % bbox)
            self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_index_equal_coords_bbox(self):
        bbox = '2606900,1228600,2606900,1228600'
        status_code, json_data = self.get("/%s/?bbox=%s" % (self.dataset, bbox))
        self.assertEqual(200, status_code, "Status code is not OK")
        self.assertEqual('FeatureCollection', json_data['type'])

    # show

    def test_show(self):
        status_code, json_data = self.get("/%s/1" % self.dataset)
        self.assertEqual(200, status_code, "Status code is not OK")
        self.check_feature(json_data)
        self.assertEqual(1, json_data['id'], "ID does not match")

    def test_show_read_only(self):
        status_code, json_data = self.get("/%s/1" % self.dataset_read_only)
        self.assertEqual(200, status_code, "Status code is not OK")
        self.check_feature(json_data)
        self.assertEqual(1, json_data['id'], "ID does not match")

    def test_show_invalid_dataset(self):
        status_code, json_data = self.get('/test/1')
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Dataset not found or permission error', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_show_invalid_id(self):
        status_code, json_data = self.get("/%s/999999" % self.dataset)
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Feature not found', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    # create

    def test_create(self):
        input_feature = self.build_poly_feature()
        status_code, json_data = self.post("/%s/" % self.dataset, input_feature)
        self.assertEqual(201, status_code, "Status code is not Created")
        feature = json_data
        self.check_feature(feature)
        self.assertEqual(input_feature['properties'], feature['properties'],
                         "Properties do not match")
        self.assertEqual(input_feature['geometry'], feature['geometry'],
                         "Geometry does not match")

        # check that feature has been created
        status_code, json_data = self.get(
            "/%s/%d" % (self.dataset, feature['id']))
        self.assertEqual(200, status_code, "Status code is not OK")
        self.assertEqual(feature, json_data)

    def test_create_read_only(self):
        input_feature = self.build_point_feature()
        status_code, json_data = self.post("/%s/" % self.dataset_read_only,
                                           input_feature)
        self.assertEqual(405, status_code,
                         "Status code is not Method Not Allowed")
        self.assertEqual('Dataset not writable', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_create_invalid_dataset(self):
        input_feature = self.build_poly_feature()
        status_code, json_data = self.post('/invalid_dataset/', input_feature)
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Dataset not found or permission error', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    # update

    def test_update(self):
        input_feature = self.build_poly_feature()
        status_code, json_data = self.put("/%s/1" % self.dataset, input_feature)
        self.assertEqual(200, status_code, "Status code is not OK")
        feature = json_data
        self.check_feature(feature)
        self.assertEqual(1, feature['id'], "ID does not match")
        self.assertEqual(input_feature['properties'], feature['properties'],
                         "Properties do not match")
        self.assertEqual(input_feature['geometry'], feature['geometry'],
                         "Geometry does not match")

        # check that feature has been updated
        status_code, json_data = self.get(
            "/%s/%d" % (self.dataset, feature['id']))
        self.assertEqual(200, status_code, "Status code is not OK")
        self.assertEqual(feature, json_data)

    def test_update_read_only(self):
        input_feature = self.build_point_feature()
        status_code, json_data = self.put("/%s/1" % self.dataset_read_only,
                                          input_feature)
        self.assertEqual(405, status_code,
                         "Status code is not Method Not Allowed")
        self.assertEqual('Dataset not writable', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_update_invalid_dataset(self):
        input_feature = self.build_poly_feature()
        status_code, json_data = self.put('/invalid_dataset/1', input_feature)
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Dataset not found or permission error', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_update_invalid_id(self):
        input_feature = self.build_poly_feature()
        status_code, json_data = self.put(
            "/%s/999999" % self.dataset, input_feature)
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Feature not found', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    # destroy

    def test_destroy(self):
        status_code, json_data = self.delete("/%s/2" % self.dataset)
        self.assertEqual(200, status_code, "Status code is not OK")
        self.assertEqual('Dataset feature deleted', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

        # check that feature has been deleted
        status_code, json_data = self.get("/%s/2" % self.dataset)
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Feature not found', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_destroy_read_only(self):
        status_code, json_data = self.delete("/%s/2" % self.dataset_read_only)
        self.assertEqual(405, status_code,
                         "Status code is not Method Not Allowed")
        self.assertEqual('Dataset not writable', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_destroy_invalid_dataset(self):
        status_code, json_data = self.delete('/test/1')
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Dataset not found or permission error', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")

    def test_destroy_invalid_id(self):
        status_code, json_data = self.delete(
            "/%s/999999" % self.dataset)
        self.assertEqual(404, status_code, "Status code is not Not Found")
        self.assertEqual('Feature not found', json_data['message'],
                         "Message does not match")
        self.assertNotIn('type', json_data, "GeoJSON Type present")
