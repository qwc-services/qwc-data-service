import os
import unittest

from werkzeug.datastructures import LanguageAccept
from flask.logging import logging
from qwc_services_core.database import DatabaseEngine
from qwc_services_core.translator import Translator
from dataset_features_provider import DatasetFeaturesProvider

class FakeRequest:
    @property
    def accept_languages(self):
        return LanguageAccept([('en', 1)])

class FakeApp:
    @property
    def logger(self):
        return logging.getLogger()

    @property
    def root_path(self):
        return os.path.join(os.path.dirname(__file__), "..")

class FeatureValidationTestCase(unittest.TestCase):
    """Test case for feature validations"""

    def setUp(self):
        self.db_engine = DatabaseEngine()
        self.translator = Translator(FakeApp(), FakeRequest())

    def tearDown(self):
        pass

    def build_config(self, merge_config={}):
        """Create data service config

        :param dict merge_config: Partial config data to merge into
                                       basic config
        """
        config = {
            'dataset': 'test_points',
            'schema': 'public',
            'table_name': 'test_points',
            'primary_key': 'id',
            'geometry_column': 'geom',
            'geometry_type': 'POINT',
            'srid': 3857,
            'allow_null_geometry': False,
            'attributes': [
                'field'
            ],
            'fields': {
                'field': {}
            },
            'writable': False
        }
        config.update(merge_config)
        return config

    def build_feature(self, merge_feature={}, srid=3857):
        """Create GeoJSON feature

        :param dict merge_feature: Partial feature data to merge into basic
                                   feature
        :param int srid: optional SRID (default: 3857)
        """
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [950758.0, 6003950.0]
            },
            'properties': {
                'field': None
            },
            'crs': {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::%d' % srid
                }
            }
        }
        feature.update(merge_feature)
        return feature

    def test_invalid_input_values(self):
        """Test different input values and types for different
        field data types"""
        config = self.build_config()
        feature = self.build_feature()

        input_tests = [
            # ['<data_type>', [<valid values>], [<invalid values>]]
            ['bigint', [None, 123, -456, 78.91, 9223372036854775807, -9223372036854775808, "123"], [9223372036854775808, -9223372036854775809, "abc", True]],
            ['boolean', [None, True, "1", "True", "true", "TRUE"], [1, 0, 123, "abc", 1.0, 0.0, 78.91]],
            ['character varying', [None, "abc", "123", 123, -456, 78.91, True], []],
            ['date', [None, "2018-08-21", "2018/08/21", "20180821", "2018-08-21 13:00:00"], [123, 78.91, "abc", True]],
            ['double precision', [None, 78.91, 123, -456, "123", "78.91"], ["abc", True]],
            ['integer', [None, 123, -456, 78.91, 2147483647, -2147483648, "123", True], [2147483648, -2147483649, "abc"]],
            ['json', [None, "abc", "123", 123, -456, 78.91, True, {"a": 1}, ["b"], {"c": {"d": 123}}], []],
            ['numeric(5,2)', [None, 123, -456, 78.91, 999.99, -999.99, "123"], [999.995, -999.995, 2147483647, "abc", True]],
            ['real', [None, 123, -456, 78.91, "123", "78.91"], ["abc", True]],
            ['smallint', [None, 123, -456, 78.91, 32767, -32768, "123"], [32768, -32769, "abc", True]],
            ['text', [None, "abc", "123", 123, -456, 78.91, True], []],
            ['time', [None, "13:00:00", "13:00", "2018-08-21 13:00:00"], [123, 78.91, "abc", "2018-08-21", True]],
            ['timestamp with time zone', [None, "2018-08-21", "2018/08/21", "20180821", "2018-08-21 13:00:00", "2018-08-21 13:00:00+2"], [123, 78.91, "abc", True]],
            ['timestamp without time zone', [None, "2018-08-21", "2018/08/21", "20180821", "2018-08-21 13:00:00", "2018-08-21 13:00:00+2"], [123, 78.91, "abc", True]],
            ['uuid', [None, "e8a1d388-3138-4907-88fd-d8f92826cfb9", "e8a1d3883138490788fdd8f92826cfb9"], [123, 78.91, "abc", "z8a1d388-3138-4907-88fd-d8f92826cfb9", "8a1d388-3138-4907-88fd-d8f92826cfb9", True]]
        ]
        for input_test in input_tests:
            data_type = input_test[0]
            valid_values = input_test[1]
            invalid_values = input_test[2]

            # setup config with data type
            config['fields']['field']['data_type'] = data_type
            if data_type.startswith('numeric'):
                # use numeric(5,2)
                config['fields']['field']['constraints'] = {
                    'numeric_precision': 5,
                    'numeric_scale': 2
                }
            dataset_features_provider = DatasetFeaturesProvider(
                config, self.db_engine, logging.getLogger(), self.translator
            )

            for value in valid_values:
                # validate feature with valid input value
                feature['properties']['field'] = value
                errors = dataset_features_provider.validate(feature)

                self.assertNotIn(
                    'data_errors', errors,
                    "Unexpected data errors (%s: %s)" % (data_type, value)
                )

            for value in invalid_values:
                # validate feature with invalid input value
                feature['properties']['field'] = value
                errors = dataset_features_provider.validate(feature)

                self.assertIn(
                    'data_errors', errors,
                    "Missing data errors (%s: %s)" % (data_type, value)
                )
                self.assertIn(
                    "Invalid value for 'field' for type %s" % data_type,
                    errors['data_errors'],
                    "Data errors do not match (%s: %s)" % (data_type, value)
                )

    def test_field_constraints(self):
        """Test field constraints"""
        config = self.build_config()
        feature = self.build_feature()

        error_msg_maxlength = "Value for 'field' must be shorter than {maxlength} characters"
        error_msg_min = "Value for 'field' must be greater than or equal to {min}"
        error_msg_max = "Value for 'field' must be less than or equal to {max}"
        error_msg_values = "Invalid value for 'field'"

        input_tests = [
            # ['<data_type>', <constraints>, [<valid values>], [<invalid values>], <expected error message>]
            ['character varying', {'maxlength': 8}, ["abc", "abcdefgh"], ["abcdefghi"], error_msg_maxlength.format(maxlength=8)],
            ['integer', {'min': -10}, [-10, 20], [-11], error_msg_min.format(min=-10)],
            ['integer', {'max': 20}, [-10, 20], [21], error_msg_max.format(max=20)],
            ['character varying', {
                'values': [
                    {'label': 'A', 'value': 'abc'},
                    {'label': 'B', 'value': 'def'},
                    {'label': 'C', 'value': '123'},
                ]
            }, ["abc", "def", "123", 123], ["A", "Abc", "e", 456], error_msg_values],
        ]

        for input_test in input_tests:
            data_type = input_test[0]
            constraints = input_test[1]
            valid_values = input_test[2]
            invalid_values = input_test[3]
            error_msg = input_test[4]

            # setup config with data type and constraints
            config['fields']['field'].update({
                'data_type': data_type,
                'constraints': constraints
            })
            dataset_features_provider = DatasetFeaturesProvider(
                config, self.db_engine, logging.getLogger(), self.translator
            )

            for value in valid_values:
                # validate feature with valid input value
                feature['properties']['field'] = value
                errors = dataset_features_provider.validate(feature)

                self.assertNotIn(
                    'data_errors', errors,
                    "Unexpected data errors (%s: %s)" % (data_type, value)
                )

            for value in invalid_values:
                # validate feature with invalid input value
                feature['properties']['field'] = value
                errors = dataset_features_provider.validate(feature)

                self.assertIn(
                    'data_errors', errors,
                    "Missing data errors (%s: %s)" % (data_type, value)
                )
                self.assertIn(
                    error_msg,
                    errors['data_errors'],
                    "Data errors do not match (%s: %s)" % (data_type, value)
                )

    def test_read_only_constraint(self):
        """Test read-only constraint"""
        config = self.build_config({
            'attributes': [
                'read_only_field',
                'field'
            ],
            'fields': {
                'read_only_field': {
                    'data_type': 'integer',
                    'constraints': {'readOnly': True}
                },
                'field': {
                    'data_type': 'integer'
                }
            }
        })
        feature = self.build_feature({
            'properties': {
                'read_only_field': 123,
                'field': 456
            }
        })

        dataset_features_provider = DatasetFeaturesProvider(
            config, self.db_engine, logging.getLogger(), self.translator
        )
        errors = dataset_features_provider.validate(feature)

        self.assertNotIn('data_errors', errors, "Unexpected data errors")
        self.assertNotIn(
            'read_only_field', feature['properties'],
            "Read-only property has not been removed"
        )
        self.assertIn(
            'field', feature['properties'],
            "Writable property has been removed"
        )

    def test_required_constraint(self):
        """Test 'required' constraint"""
        config = self.build_config({
            'attributes': [
                'required_field',
                'field'
            ],
            'fields': {
                'required_field': {
                    'data_type': 'integer',
                    'constraints': {'required': True}
                },
                'field': {
                    'data_type': 'integer'
                }
            }
        })
        feature = self.build_feature()

        dataset_features_provider = DatasetFeaturesProvider(
            config, self.db_engine, logging.getLogger(), self.translator
        )

        # required field not in properties
        errors = dataset_features_provider.validate(feature)
        self.assertIn('data_errors', errors, "Missing data errors")
        self.assertIn(
            "Missing required value for 'required_field'",
            errors['data_errors'],
            "Data errors do not match"
        )

        # required value is None
        feature['properties']['required_field'] = None
        errors = dataset_features_provider.validate(feature)
        self.assertIn('data_errors', errors, "Missing data errors")
        self.assertIn(
            "Missing required value for 'required_field'",
            errors['data_errors'],
            "Data errors do not match"
        )

        # required value is blank
        feature['properties']['required_field'] = ""
        errors = dataset_features_provider.validate(feature)
        self.assertIn('data_errors', errors, "Missing data errors")
        self.assertIn(
            "Value for 'required_field' can not be blank",
            errors['data_errors'],
            "Data errors do not match"
        )

        # required value is set
        feature['properties']['required_field'] = 123
        errors = dataset_features_provider.validate(feature)
        self.assertNotIn('data_errors', errors, "Unexpected data errors")
