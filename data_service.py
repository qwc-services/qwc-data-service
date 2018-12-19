from collections import OrderedDict

from sqlalchemy.exc import DataError, InternalError, ProgrammingError

from qwc_services_core.database import DatabaseEngine
from qwc_services_core.permission import PermissionClient
from dataset_features_provider import DatasetFeaturesProvider


class DataService():
    """DataService class

    Manage reading and writing of dataset features.
    """

    def __init__(self, logger):
        """Constructor

        :param Logger logger: Application logger
        """
        self.logger = logger
        self.db_engine = DatabaseEngine()
        self.permission = PermissionClient()

    def index(self, identity, dataset, bbox, crs, filterexpr):
        """Find dataset features inside bounding box.

        :param str identity: User identity
        :param str dataset: Dataset ID
        :param str bbox: Bounding box as '<minx>,<miny>,<maxx>,<maxy>' or None
        :param str crs: Client CRS as 'EPSG:<srid>' or None
        :param str filterexpr: Comma-separated filter expressions as
                               '<k1> = <v1>, <k2> like <v2>, ...'
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, dataset
        )
        if dataset_features_provider is not None:
            # check read permission
            if not dataset_features_provider.readable():
                return {
                    'error': "Dataset not readable",
                    'error_code': 405
                }

            if bbox is not None:
                # parse and validate input bbox
                bbox = dataset_features_provider.parse_bbox(bbox)
                if bbox is None:
                    return {
                        'error': "Invalid bounding box",
                        'error_code': 400
                    }
            srid = None
            if crs is not None:
                # parse and validate unput CRS
                srid = dataset_features_provider.parse_crs(crs)
                if srid is None:
                    return {
                        'error': "Invalid CRS",
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
                bbox, srid, filterexpr
            )
            return {'feature_collection': feature_collection}
        else:
            return {'error': "Dataset not found or permission error"}

    def show(self, identity, dataset, id, crs):
        """Get a dataset feature.

        :param str identity: User identity
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        :param str crs: Client CRS as 'EPSG:<srid>' or None
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, dataset
        )
        srid = None
        if crs is not None:
            # parse and validate unput CRS
            srid = dataset_features_provider.parse_crs(crs)
            if srid is None:
                return {
                    'error': "Invalid CRS",
                    'error_code': 400
                }
        if dataset_features_provider is not None:
            # check read permission
            if not dataset_features_provider.readable():
                return {
                    'error': "Dataset not readable",
                    'error_code': 405
                }

            feature = dataset_features_provider.show(id, srid)
            if feature is not None:
                return {'feature': feature}
            else:
                return {'error': "Feature not found"}
        else:
            return {'error': "Dataset not found or permission error"}

    def create(self, identity, dataset, feature):
        """Create a new dataset feature.

        :param str identity: User identity
        :param str dataset: Dataset ID
        :param object feature: GeoJSON Feature
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, dataset
        )
        if dataset_features_provider is not None:
            # check create permission
            if not dataset_features_provider.creatable():
                return {
                    'error': "Dataset not creatable",
                    'error_code': 405
                }

            # validate input feature
            validation_errors = dataset_features_provider.validate(feature)
            if not validation_errors:
                # create new feature
                try:
                    feature = dataset_features_provider.create(feature)
                except (DataError, InternalError, ProgrammingError) as e:
                    self.logger.error(e)
                    return {
                        'error': "Feature commit failed",
                        'error_details': {
                            'data_errors': ["Feature could not be created"],
                        },
                        'error_code': 422
                    }
                return {'feature': feature}
            else:
                return {
                    'error': "Feature validation failed",
                    'error_details': validation_errors,
                    'error_code': 422
                }
        else:
            return {'error': "Dataset not found or permission error"}

    def update(self, identity, dataset, id, feature):
        """Update a dataset feature.

        :param str identity: User identity
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        :param object feature: GeoJSON Feature
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, dataset
        )
        if dataset_features_provider is not None:
            # check update permission
            if not dataset_features_provider.updatable():
                return {
                    'error': "Dataset not updatable",
                    'error_code': 405
                }

            # validate input feature
            validation_errors = dataset_features_provider.validate(feature)
            if not validation_errors:
                # update feature
                try:
                    feature = dataset_features_provider.update(id, feature)
                except (DataError, InternalError, ProgrammingError) as e:
                    self.logger.error(e)
                    return {
                        'error': "Feature commit failed",
                        'error_details': {
                            'data_errors': ["Feature could not be updated"],
                        },
                        'error_code': 422
                    }
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

    def destroy(self, identity, dataset, id):
        """Delete a dataset feature.

        :param str identity: User identity
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, dataset
        )
        if dataset_features_provider is not None:
            # check delete permission
            if not dataset_features_provider.deletable():
                return {
                    'error': "Dataset not deletable",
                    'error_code': 405
                }

            if dataset_features_provider.destroy(id):
                return {}
            else:
                return {'error': "Feature not found"}
        else:
            return {'error': "Dataset not found or permission error"}

    def dataset_features_provider(self, identity, dataset):
        """Return DatasetFeaturesProvider if available and permitted.

        :param str identity: User identity
        :param str dataset: Dataset ID
        """
        dataset_features_provider = None

        # check permissions (NOTE: returns None on error)
        permissions = self.permission.dataset_edit_permissions(
            dataset, identity
        )
        if permissions is not None and permissions:
            # create DatasetFeaturesProvider
            dataset_features_provider = DatasetFeaturesProvider(
                permissions, self.db_engine
            )

        return dataset_features_provider
