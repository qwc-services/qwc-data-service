from collections import OrderedDict

from qwc_services_core.database import DatabaseEngine
from qwc_services_core.permission import PermissionClient
from qwc_services_core.cache import Cache
from dataset_features_provider import DatasetFeaturesProvider


class DataService():
    """DataService class

    Manage reading and writing of dataset features.
    """

    def __init__(self):
        """Constructor"""
        self.db_engine = DatabaseEngine()
        self.permission = PermissionClient()
        # Cache for DatasetFeaturesProvider instances
        self.dataset_features_providers = Cache()

    def index(self, username, dataset, bbox, filterexpr):
        """Find dataset features inside bounding box.

        :param str username: User name
        :param str dataset: Dataset ID
        :param str bbox: Bounding box as '<minx>,<miny>,<maxx>,<maxy>' or None
        :param str filterexpr: Comma-separated filter expressions as
                               '<k1> = <v1>, <k2> like <v2>, ...'
        """
        dataset_features_provider = self.dataset_features_provider(
            username, dataset
        )
        if dataset_features_provider is not None:
            if bbox is not None:
                # parse and validate input bbox
                bbox = dataset_features_provider.parse_bbox(bbox)
                if bbox is None:
                    return {
                        'error': "Invalid bounding box",
                        'error_code': 400
                    }
            if filterexpr is not None:
                # parse and validate input filter
                filterexpr = dataset_features_provider.parse_filter(filterexpr)
                if filterexpr is None:
                    return {
                        'error': "Invalid filter expression",
                        'error_code': 400
                    }

            feature_collection = dataset_features_provider.index(
                bbox, filterexpr
            )
            return {'feature_collection': feature_collection}
        else:
            return {'error': "Dataset not found or permission error"}

    def show(self, username, dataset, id):
        """Get a dataset feature.

        :param str username: User name
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        """
        dataset_features_provider = self.dataset_features_provider(
            username, dataset
        )
        if dataset_features_provider is not None:
            feature = dataset_features_provider.show(id)
            if feature is not None:
                return {'feature': feature}
            else:
                return {'error': "Feature not found"}
        else:
            return {'error': "Dataset not found or permission error"}

    def create(self, username, dataset, feature):
        """Create a new dataset feature.

        :param str username: User name
        :param str dataset: Dataset ID
        :param object feature: GeoJSON Feature
        """
        dataset_features_provider = self.dataset_features_provider(
            username, dataset
        )
        if dataset_features_provider is not None:
            # check write permission
            if dataset_features_provider.is_read_only():
                return {
                    'error': "Dataset not writable",
                    'error_code': 405
                }

            # validate input feature
            validation_errors = dataset_features_provider.validate(feature)
            if not validation_errors:
                # create new feature
                feature = dataset_features_provider.create(feature)
                return {'feature': feature}
            else:
                return {
                    'error': "Feature validation failed",
                    'error_details': validation_errors,
                    'error_code': 422
                }
        else:
            return {'error': "Dataset not found or permission error"}

    def update(self, username, dataset, id, feature):
        """Update a dataset feature.

        :param str username: User name
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        :param object feature: GeoJSON Feature
        """
        dataset_features_provider = self.dataset_features_provider(
            username, dataset
        )
        if dataset_features_provider is not None:
            # check write permission
            if dataset_features_provider.is_read_only():
                return {
                    'error': "Dataset not writable",
                    'error_code': 405
                }

            # validate input feature
            validation_errors = dataset_features_provider.validate(feature)
            if not validation_errors:
                # update feature
                feature = dataset_features_provider.update(id, feature)
                if feature is not None:
                    return {'feature': feature}
                else:
                    return {'error': "Feature not found"}
            else:
                return {
                    'error': "Feature validation failed",
                    'error_details': validation_errors,
                    'error_code': 422
                }
        else:
            return {'error': "Dataset not found or permission error"}

    def destroy(self, username, dataset, id):
        """Delete a dataset feature.

        :param str username: User name
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        """
        dataset_features_provider = self.dataset_features_provider(
            username, dataset
        )
        if dataset_features_provider is not None:
            # check write permission
            if dataset_features_provider.is_read_only():
                return {
                    'error': "Dataset not writable",
                    'error_code': 405
                }

            if dataset_features_provider.destroy(id):
                return {}
            else:
                return {'error': "Feature not found"}
        else:
            return {'error': "Dataset not found or permission error"}

    def dataset_features_provider(self, username, dataset):
        """Return DatasetFeaturesProvider if available and permitted.

        :param str username: User name
        :param str dataset: Dataset ID
        """
        dataset_features_provider = self.dataset_features_providers.read(
            dataset, username, [])
        if dataset_features_provider is None:
            # dataset not yet cached
            # check permissions (NOTE: returns None on error)
            permissions = self.permission.dataset_edit_permissions(
                dataset, username
            )
            if permissions is not None:
                if permissions:
                    # create DatasetFeaturesProvider
                    dataset_features_provider = DatasetFeaturesProvider(
                        permissions, self.db_engine
                    )
                else:
                    dataset_features_provider = False

                # add to cache
                # NOTE: False if not available or not permitted
                self.dataset_features_providers.write(
                    dataset, username, [], dataset_features_provider, 300)

        if dataset_features_provider is False:
            dataset_features_provider = None
        return dataset_features_provider
