import os
from datetime import datetime
from collections import OrderedDict

from sqlalchemy.exc import (DataError, IntegrityError,
                            InternalError, ProgrammingError)

from qwc_services_core.auth import get_username
from qwc_services_core.database import DatabaseEngine
from qwc_services_core.permissions_reader import PermissionsReader
from dataset_features_provider import DatasetFeaturesProvider
from attachments_service import AttachmentsService

ERROR_DETAILS_LOG_ONLY = os.environ.get(
    'ERROR_DETAILS_LOG_ONLY', 'False').lower() == 'true'


class DataService():
    """DataService class

    Manage reading and writing of dataset features.
    """

    def __init__(self, tenant, logger, config):
        """Constructor

        :param str tenant: Tenant ID
        :param Logger logger: Application logger
        """
        self.tenant = tenant
        self.logger = logger
        self.config = config
        self.resources = self.load_resources()
        self.permissions_handler = PermissionsReader(tenant, logger)
        self.attachments_service = AttachmentsService(tenant, logger)
        self.db_engine = DatabaseEngine()

    def index(self, identity, translator, dataset, bbox, crs, filterexpr, filter_geom):
        """Find dataset features inside bounding box.

        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param str bbox: Bounding box as '<minx>,<miny>,<maxx>,<maxy>' or None
        :param str crs: Client CRS as 'EPSG:<srid>' or None
        :param str filterexpr: JSON serialized array of filter expressions:
        [["<attr>", "<op>", "<value>"], "and|or", ["<attr>", "<op>", "<value>"]]
        :param str filter_geom: JSON serialized GeoJSON geometry
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, False
        )
        if dataset_features_provider is not None:
            # check read permission
            if not dataset_features_provider.readable():
                return {
                    'error': translator.tr("error.dataset_not_readable"),
                    'error_code': 405
                }

            if bbox is not None:
                # parse and validate input bbox
                bbox = dataset_features_provider.parse_bbox(bbox)
                if bbox is None:
                    return {
                        'error': translator.tr("error.invalid_bounding_box"),
                        'error_code': 400
                    }
            srid = None
            if crs is not None:
                # parse and validate input CRS
                srid = dataset_features_provider.parse_crs(crs)
                if srid is None:
                    return {
                        'error': translator.tr("error.invalid_crs"),
                        'error_code': 400
                    }
            if filterexpr is not None:
                # parse and validate input filter
                filterexpr = dataset_features_provider.parse_filter(filterexpr)
                if filterexpr[0] is None:
                    return {
                        'error': (
                            translator.tr("error.invalid_filter_expression") + ": " + filterexpr[1]
                        ),
                        'error_code': 400
                    }

            try:
                feature_collection = dataset_features_provider.index(
                    bbox, srid, filterexpr, filter_geom
                )
            except (DataError, ProgrammingError) as e:
                self.logger.error(e)
                return {
                    'error': (
                        translator.tr("error.feature_query_failed")
                    ),
                    'error_code': 400
                }
            return {'feature_collection': feature_collection}
        else:
            return {'error': translator.tr("error.dataset_not_found")}

    def extent(self, identity, translator, dataset, crs, filterexpr, filter_geom):
        """Get extent of dataset features.

        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param str crs: Client CRS as 'EPSG:<srid>' or None
        :param str filterexpr: JSON serialized array of filter expressions:
        [["<attr>", "<op>", "<value>"], "and|or", ["<attr>", "<op>", "<value>"]]
        :param str filter_geom: JSON serialized GeoJSON geometry
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, False
        )
        if dataset_features_provider is not None:
            # check read permission
            if not dataset_features_provider.readable():
                return {
                    'error': translator.tr("error.dataset_not_readable"),
                    'error_code': 405
                }

            srid = None
            if crs is not None:
                # parse and validate input CRS
                srid = dataset_features_provider.parse_crs(crs)
                if srid is None:
                    return {
                        'error': translator.tr("error.invalid_crs"),
                        'error_code': 400
                    }
            if filterexpr is not None:
                # parse and validate input filter
                filterexpr = dataset_features_provider.parse_filter(filterexpr)
                if filterexpr[0] is None:
                    return {
                        'error': (
                            translator.tr("error.invalid_filter_expression") % filterexpr[1]
                        ),
                        'error_code': 400
                    }

            try:
                extent = dataset_features_provider.extent(
                    srid, filterexpr, filter_geom
                )
            except (DataError, ProgrammingError) as e:
                self.logger.error(e)
                return {
                    'error': (
                        translator.tr("error.feature_query_failed")
                    ),
                    'error_code': 400
                }
            return {'extent': {'bbox': extent}}
        else:
            return {'error': translator.tr("error.dataset_not_found")}

    def show(self, identity, translator, dataset, id, crs):
        """Get a dataset feature.

        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        :param str crs: Client CRS as 'EPSG:<srid>' or None
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, False
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
                    'error': translator.tr("error.dataset_not_readable"),
                    'error_code': 405
                }

            feature = dataset_features_provider.show(id, srid)
            if feature is not None:
                return {'feature': feature}
            else:
                return {'error': translator.tr("error.feature_not_found")}
        else:
            return {'error': translator.tr("error.dataset_not_found")}

    def create(self, identity, translator, dataset, feature, files={}):
        """Create a new dataset feature.

        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param object feature: GeoJSON Feature
        :param object files: Upload files
        """

        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, True
        )
        if dataset_features_provider is None:
            return {'error': translator.tr("error.dataset_not_found")}

        # check create permission
        if not dataset_features_provider.creatable():
            return {
                'error': translator.tr("error.dataset_not_creatable"),
                'error_code': 405
            }

        # validate input feature and attachments
        validation_errors = dataset_features_provider.validate(
            feature, new_feature=True
        )
        validation_errors.update(self.validate_attachments(translator, files, dataset_features_provider, dataset))

        if validation_errors:
            return self.error_response(
                translator.tr("error.feature_validation_failed"), validation_errors)

        # Save attachments
        saved_attachments = {}
        save_errors = self.save_attachments(translator, files, dataset, feature, identity, saved_attachments)
        if save_errors:
            return self.error_response(translator.tr("error.feature_commit_failed"), save_errors)

        self.add_logging_fields(feature, identity)

        # create new feature
        try:
            feature = dataset_features_provider.create(feature)
        except (DataError, IntegrityError,
                InternalError, ProgrammingError) as e:
            self.logger.error(e)
            reason = translator.tr("error.feature_could_not_be_created")
            if isinstance(e, IntegrityError):
                reason += ": " + e.orig.diag.message_detail
            elif isinstance(e, InternalError):
                reason += ": " + e.orig.diag.message_primary
            for slug in saved_attachments.values():
                self.attachments_service.remove_attachment(dataset, slug)
            return {
                'error': translator.tr("error.feature_commit_failed"),
                'error_details': {
                    'data_errors': [reason],
                },
                'error_code': 422
            }
        return {'feature': feature}

    def update(self, identity, translator, dataset, id, feature, files={}):
        """Update a dataset feature.

        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        :param object feature: GeoJSON Feature
        :param object files: Upload files
        """

        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, True
        )
        if dataset_features_provider is None:
            return {'error': translator.tr("error.dataset_not_found")}

        # check update permission
        if not dataset_features_provider.updatable():
            return {
                'error': translator.tr("error.dataset_not_updatable"),
                'error_code': 405
            }

        # validate input feature and attachments
        validation_errors = dataset_features_provider.validate(feature)
        validation_errors.update(self.validate_attachments(translator, files, dataset_features_provider, dataset))

        if validation_errors:
            return self.error_response(
                translator.tr("error.feature_validation_failed"), validation_errors)

        if validation_errors:
            return self.error_response(
                translator.tr("error.feature_validation_failed"), validation_errors)

        # Save attachments
        saved_attachments = {}
        save_errors = self.save_attachments(translator, files, dataset, feature, identity, saved_attachments)
        if save_errors:
            return self.error_response(translator.tr("error.feature_commit_failed"), save_errors)

        # Cleanup previous attachments
        upload_user_field_suffix = self.config.get("upload_user_field_suffix", None)
        show_result = self.show(identity, translator, dataset, id, None)
        for key, value in show_result.get('feature', {}).get('properties', {}).items():
            if isinstance(value, str) and value.startswith("attachment://") and feature["properties"][key] != value:
                self.attachments_service.remove_attachment(dataset, value[13:])
                if upload_user_field_suffix:
                    upload_user_field = key + "__" + upload_user_field_suffix
                    feature["properties"][upload_user_field] = get_username(identity)

        self.add_logging_fields(feature, identity)

        # update feature
        try:
            feature = dataset_features_provider.update(id, feature)
        except (DataError, IntegrityError,
                InternalError, ProgrammingError) as e:
            self.logger.error(e)
            reason = translator.tr("error.feature_could_not_be_updated")
            if isinstance(e, IntegrityError):
                reason += ": " + e.orig.diag.message_detail
            elif isinstance(e, InternalError):
                reason += ": " + e.orig.diag.message_primary
            for slug in saved_attachments.values():
                attachments.remove_attachment(dataset, slug)
            return {
                'error': translator.tr("error.feature_commit_failed"),
                'error_details': {
                    'data_errors': [reason],
                },
                'error_code': 422
            }
        if feature is not None:
            return {'feature': feature}
        else:
            return {'error': translator.tr("error.feature_not_found")}

    def destroy(self, identity, translator, dataset, id):
        """Delete a dataset feature.

        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, False
        )
        if dataset_features_provider is None:
            return {'error': translator.tr("error.dataset_not_found")}

        # check delete permission
        if not dataset_features_provider.deletable():
            return {
                'error': translator.tr("error.dataset_not_deletable"),
                'error_code': 405
            }

        show_result = self.show(identity, translator, dataset, id, None)

        if not dataset_features_provider.destroy(id):
            return {'error': translator.tr("error.feature_not_found")}

        # cleanup attachments
        for key, value in show_result.get('feature', {}).get('properties', {}).items():
            if isinstance(value, str) and value.startswith("attachment://"):
                self.attachments_service.remove_attachment(dataset, value[13:])

        return {}

    def is_editable(self, identity, translator, dataset, id):
        """Returns whether a dataset is editable.
        :param object identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param int id: Dataset feature ID
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, False
        )
        if dataset_features_provider is not None:
            # check update permission
            if not dataset_features_provider.updatable():
                return False

        return dataset_features_provider.exists(id)

    def dataset_features_provider(self, identity, translator, dataset, write):
        """Return DatasetFeaturesProvider if available and permitted.

        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param bool write: Whether to include permissions relevant for writing to the dataset (create/update)
        """
        dataset_features_provider = None

        self.logger.debug("checking edit permissions for dataset")
        permissions = self.dataset_edit_permissions(
            dataset, identity, translator, write
        )
        if permissions:
            self.logger.debug("create DatasetFeaturesProvider")
            dataset_features_provider = DatasetFeaturesProvider(
                permissions, self.db_engine, self.logger, translator
            )

        return dataset_features_provider

    def load_resources(self):
        """Load service resources from config."""
        # get service resources
        datasets = {}
        for resource in self.config.resources().get('datasets', []):
            datasets[resource['name']] = resource

        return {
            'datasets': datasets
        }

    def dataset_edit_permissions(self, dataset, identity, translator, write):
        """Return dataset edit permissions if available and permitted.
        Includes permitted resources with field metadata and keyvalrels

        :param str dataset: Dataset ID
        :param str|obj identity: User identity
        :param object translator: Translator
        :param bool write: Whether to include permissions relevant for writing to the dataset (create/update)
        """
        # find resource for requested dataset
        resource = self.resources['datasets'].get(dataset)
        if resource is None:
            # dataset not found
            return {}

        # get permissions for dataset
        if resource.get('readonlypermitted', False):
            writable = False
            creatable = False
            readable = True
            updatable = False
            deletable = False
            permitted_attributes = list(map(lambda field: field["name"], resource['fields']))

        else:
            resource_permissions = self.permissions_handler.resource_permissions(
                'data_datasets', identity, dataset
            )
            if not resource_permissions:
                # dataset not permitted
                return {}

            # combine permissions
            permitted_attributes = set()
            writable = False
            creatable = False
            readable = False
            updatable = False
            deletable = False

            for permission in resource_permissions:
                # collect permitted attributes
                permitted_attributes.update(permission.get('attributes', []))

                # allow writable and CRUD actions if any role permits them
                writable |= permission.get('writable', False)
                creatable |= permission.get('creatable', False)
                readable |= permission.get('readable', False)
                updatable |= permission.get('updatable', False)
                deletable |= permission.get('deletable', False)

            # make writable consistent with CRUD actions
            writable |= creatable and readable and updatable and deletable

            # make CRUD actions consistent with writable
            creatable |= writable
            readable |= writable
            updatable |= writable
            deletable |= writable

            permitted = creatable or readable or updatable or deletable
            if not permitted:
                # no CRUD action permitted
                return {}

        # filter by permissions
        attributes = [
            field['name'] for field in resource['fields']
            if field['name'] in permitted_attributes
        ]

        fields = {}
        for field in resource['fields']:
            if field['name'] in permitted_attributes:
                fields[field['name']] = field

                # Resolve keyvalrels
                keyvalrel = field.get('constraints', {}).get('keyvalrel', None)
                if keyvalrel and write:
                    fields[field['name']] = dict(fields[field['name']])
                    fields[field['name']]['constraints'] = dict(fields[field['name']]['constraints'])
                    try:
                        table, key_field_name, value_field_name = keyvalrel.split(":")
                        dataset_features_provider = self.dataset_features_provider(
                            identity, translator, table, False
                        )
                        values = dataset_features_provider.keyvals(key_field_name, value_field_name)
                        fields[field['name']]['constraints']['values'] = values
                    except Exception as e:
                        self.logger.error("Unable to resolve keyvalrel '%s': %s" % (keyvalrel, str(e)))
                        fields[field['name']]['constraints']['values'] = []

        # NOTE: 'geometry' is None for datasets without geometry
        geometry = resource.get('geometry', {})

        return {
            "dataset": resource['name'],
            "database_read": resource['db_url'],
            "database_write": resource.get('db_write_url', resource['db_url']),
            "schema": resource['schema'],
            "table_name": resource['table_name'],
            "primary_key": resource['primary_key'],
            "attributes": attributes,
            "fields": fields,
            "geometry_column": geometry.get('geometry_column'),
            "geometry_type": geometry.get('geometry_type'),
            "srid": geometry.get('srid'),
            "allow_null_geometry": geometry.get('allow_null', self.config.get('geometry_default_allow_null', False)),
            "writable": writable,
            "creatable": creatable,
            "readable": readable,
            "updatable": updatable,
            "deletable": deletable,
            "jointables": resource.get('jointables', {})
        }

    def validate_attachments(self, translator, files, dataset_features_provider, dataset):
        """Validates the specified attachment files

        :param object translator: Translator
        :param list files: Uploaded files
        :param obj dataset_features_provider: Dataset features provider
        """
        attachment_errors = []
        for key in files:
            filedata = files[key]
            field = key[5:] # remove file: prefix
            attachment_valid, message = self.attachments_service.validate_attachment(translator, filedata, dataset_features_provider.fields[field], dataset)
            if not attachment_valid:
                attachment_errors.append(translator.tr("error.attachment_validation_failed") % key + ": " + message)
        if attachment_errors:
            return {
                'attachment_errors': attachment_errors
            }
        return {}

    def save_attachments(self, translator, files, dataset, feature, identity, saved_attachments):
        """Saves the specified attachment files

        :param object translator: Translator
        :param list files: Uploaded files
        :param str dataset: Dataset ID
        :param dict feature: Feature object
        :param str|obj identity: User identity
        :param dict saved_attachments: Saved attachments
        """
        upload_user_field_suffix = self.config.get("upload_user_field_suffix", None)

        for key in files:
            filedata = files[key]
            slug = self.attachments_service.save_attachment(dataset, filedata, feature["properties"])
            if not slug:
                for slug in saved_attachments.values():
                    self.attachments_service.remove_attachment(dataset, slug)
                return {'attachment_errors': [translator.tr("error.failed_to_save_attachment") + ": " + key]}
            else:
                saved_attachments[key] = slug
                field = key[5:] # remove file: prefix
                feature["properties"][field] = "attachment://" + slug
                if upload_user_field_suffix:
                    upload_user_field = field + "__" + upload_user_field_suffix
                    feature["properties"][upload_user_field] = get_username(identity)

        return {}

    def resolve_attachment(self, identity, translator, dataset, slug):
        """Retrieves the attachment file path from the specified slug
        :param str|obj identity: User identity
        :param object translator: Translator
        :param str dataset: Dataset ID
        :param str slug: Attachment slug
        """
        dataset_features_provider = self.dataset_features_provider(
            identity, translator, dataset, True
        )
        if dataset_features_provider is None:
            return {'error': translator.tr("error.dataset_not_found")}

        # check update permission
        if not dataset_features_provider.readable():
            return {
                'error': translator.tr("error.dataset_not_readable"),
                'error_code': 405
            }

        return {
            'file': self.attachments_service.resolve_attachment(dataset, slug)
        }

    def add_logging_fields(self, feature, identity):
        """Adds logging fields to the feature

        :param dict feature: Feature object
        :param str|obj identity: User identity
        """
        edit_user_field = self.config.get("edit_user_field", None)
        edit_timestamp_field = self.config.get("edit_timestamp_field", None)

        if edit_user_field:
            feature["properties"][edit_user_field] = get_username(identity)
        if edit_timestamp_field:
            feature["properties"][edit_timestamp_field] = str(datetime.now())

    def error_response(self, error, details):
        self.logger.error("%s: %s", error, details)
        if ERROR_DETAILS_LOG_ONLY:
            error_details = 'see log for details'
        else:
            error_details = details
        return {
            'error': error,
            'error_details': error_details,
            'error_code': 422
        }
