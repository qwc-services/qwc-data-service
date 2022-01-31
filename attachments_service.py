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
        self.clamav = config.get('clamd_host')

    def validate_attachment(self, dataset, file):
        """Validate file size of an attachment file.

        :param str dataset: Dataset ID
        :param FileStorage file: Attached file
        """
        try:
            # get actual file size from file,
            # as Content-Length header is usually not set
            file.seek(0, 2)
            size = file.tell()
            file.seek(0)

            if size > self.max_file_size:
                self.logger.info(
                    "File too large: %s: %d" % (file.filename, size))
                return (False, "File too large")
        except Exception as e:
            self.logger.error("Could not validate attachment: %s" % e)
            return False

        ext = os.path.splitext(file.filename)[1].lower()
        if self.allowed_extensions and ext not in self.allowed_extensions:
            self.logger.info(
                "Forbidden file extension: %s: %s" % (file.filename, ext))
            return (False, "Forbidden file extension")
        if self.clamav and scan_file(self.clamav, file.filename):
            self.logger.warn(
                "ClamAV check failed: %s" % file.filename)
            return (False, "Forbidden file content")

        return (True, None)

    def save_attachment(self, dataset, file):
        """Save attachment file for a dataset and return its slug.

        :param str dataset: Dataset ID
        :param FileStorage file: Attached file
        """
        try:
            # create target dir
            slug = self.generate_slug(20)
            target_dir = os.path.join(
                self.attachments_base_dir, self.tenant, dataset, slug)
            os.makedirs(target_dir, 0o750, True)

            # save attachment file
            filename = secure_filename(file.filename)
            file.save(os.path.join(target_dir, filename))
            self.logger.info("Saved attachment: %s" % slug)

            return slug + "/" + filename
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
