import os
import random
import string

from werkzeug.utils import secure_filename
from qwc_services_core.runtime_config import RuntimeConfig
from clamav import scan_file


class AttachmentsService():
    """AttachmentsService class

    Store attachment files for dataset features.
    """

    def __init__(self, tenant, logger):
        """Constructor

        :param str tenant: Tenant
        :param Logger logger: Application logger
        """
        self.tenant = tenant
        self.logger = logger

        config_handler = RuntimeConfig("data", self.logger)
        config = config_handler.tenant_config(self.tenant)

        self.attachments_base_dir = os.path.realpath(
            config.get('attachments_base_dir', '/tmp/qwc_attachments/'))
        self.max_file_size = int(config.get(
            'max_attachment_file_size', 10 * 1024 * 1024
        ))
        self.allowed_extensions = list(filter(lambda x: x, config.get(
            'allowed_attachment_extensions', '').split(",")))

        self.max_file_size_per_dataset = config.get(
            'max_attachment_file_size_per_dataset', {}
        )
        self.allowed_extensions_per_dataset = config.get(
            'allowed_extensions_per_dataset', {}
        )
        self.attachment_store_pattern = config.get('attachment_store_pattern', "{random}/{filename}")
        for dataset in self.allowed_extensions_per_dataset:
            self.allowed_extensions_per_dataset[dataset] = self.allowed_extensions_per_dataset[dataset].split(",")

        self.clamav = config.get('clamd_host')

    def validate_attachment(self, translator, file, fieldconfig, dataset):
        """Validate file size of an attachment file.

        :param obj translator: Translator
        :param str dataset: Dataset ID
        :param FileStorage file: Attached file
        :param dict fieldconfig: Field configuration
        """
        # Get file size
        try:
            # get actual file size from file,
            # as Content-Length header is usually not set
            file.seek(0, 2)
            size = file.tell()
            file.seek(0)
        except Exception as e:
            self.logger.error("Could not validate attachment: %s" % e)
            return False

        # Check file size:
        # - If a per dataset configuration is set, only check size against that limit
        # - Otherwise, check against global limit
        # Dataset configuration:
        if dataset in self.max_file_size_per_dataset:
            if size > self.max_file_size_per_dataset[dataset]:
                self.logger.info(
                    "File too large: %s: %d" % (file.filename, size))
                return (False, translator.tr("error.file_too_large"))
        # Global service configuration:
        elif size > self.max_file_size:
            self.logger.info(
                "File too large: %s: %d" % (file.filename, size))
            return (False, translator.tr("error.file_too_large"))


        # Get file extension
        base, ext = os.path.splitext(file.filename.lower())
        if base.endswith(".tar"):
            ext = ".tar" + ext

        # Check file extension:
        # - If a per field configuration is set, only validate against that list
        # - If a per dataset configuration is set, only validate against that list
        # - If a per global configuration is set, only validate against that list
        # Local field configuration
        fileextensions = fieldconfig.get('fileextensions', [])
        if fileextensions:
            if ext not in fileextensions:
                self.logger.info(
                    "Forbidden file extension: %s: %s" % (file.filename, ext))
                return (False, translator.tr("error.forbidden_file_extension"))
        # Dataset configuration:
        elif dataset in self.allowed_extensions_per_dataset:
            if ext not in self.allowed_extensions_per_dataset[dataset]:
                self.logger.info(
                    "Forbidden file extension: %s: %s" % (file.filename, ext))
                return (False, translator.tr("error.forbidden_file_extension"))
        # Global service configuration:
        elif self.allowed_extensions:
            if ext not in self.allowed_extensions:
                self.logger.info(
                    "Forbidden file extension: %s: %s" % (file.filename, ext))
                return (False, translator.tr("error.forbidden_file_extension"))

        # ClamAV virus check
        if self.clamav:
            result = scan_file(self.clamav, file)
            if result:
                self.logger.warn(
                    "ClamAV check failed for %s: %s" % (file.filename, result))
                return (False, translator.tr("error.forbidden_file_content"))

        return (True, None)

    def save_attachment(self, dataset, file, fields):
        """Save attachment file for a dataset and return its slug.

        :param str dataset: Dataset ID
        :param FileStorage file: Attached file
        :param dict fields: Feature fields
        """
        try:
            random = self.generate_slug(20)
            slug = self.attachment_store_pattern.format(
                random=random,
                filename=secure_filename(file.filename),
                ext=os.path.splitext(file.filename)[1],
                **fields
            )
            target_path = os.path.join(self.attachments_base_dir, self.tenant, dataset, slug)

            # create target dir
            target_dir = os.path.dirname(target_path)
            os.makedirs(target_dir, 0o755, True)

            # save attachment file
            file.save(target_path)
            self.logger.info("Saved attachment: %s" % slug)

            return slug
        except Exception as e:
            self.logger.error("Could not save attachment: %s" % e)
            return None

    def remove_attachment(self, dataset, slug):
        """Remove attachment file specified by the slug

        :param str dataset: Dataset ID
        :param slug: File slug (identifier)
        """
        target_dir = os.path.join(
            self.attachments_base_dir, self.tenant, dataset)
        try:
            os.remove(os.path.join(target_dir, slug))
            self.logger.info("Removed attachment: %s" % slug)
        except:
            self.logger.error("Could not remove attachment: %s" % slug)
            return False

        try:
            os.rmdir(os.path.join(target_dir, os.path.dirname(slug)))
        except:
            # Ignore if directory cannot be removed, is possibly non-empty
            pass
        return True

    def resolve_attachment(self, dataset, slug):
        """Resolve attachment slug to full path"""
        path = os.path.realpath(
            os.path.join(
                self.attachments_base_dir, self.tenant, dataset, slug))
        if os.path.isfile(path) and path.startswith(
                self.attachments_base_dir):
            return path
        return None

    def generate_slug(self, length):
        """Return random slug of requested length.

        :param int length: Length of slug
        """
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for c in range(length))
