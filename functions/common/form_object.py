import copy
import logging
import json

from config import (
    IMAGE_STORE_PATH,
    IMAGE_DOWNLOAD_BASE_URL,
    IMAGE_STORE_BUCKET,
)
from constant import (
    PROVIDER_ID_KEY,
    ENTRY_KEY,
    DS_ROW_ID_KEY,
    FORM_CODE_KEY,
    ANSWERS_PAGES_KEY,
    IMAGE_FILE_EXTENSIONS
)

from typing import Optional
from utils import get_from_path
from google.cloud.storage.blob import Blob
from os import path
from urllib.parse import quote_plus
from form_rule import is_passing_exclude_rules, is_passing_rules as is_passing_form_rules


class Attachment:
    """
    This class represents a form's attachment.
    """
    def __init__(self, category, type, bucket_path, download_url):
        self.category = category
        self.type = type
        self.bucket_path = bucket_path
        self.download_url = download_url


class Form:
    """
    This class represents a filled in APPEEE form/survey.
    """
    def __init__(self, data):
        provider_id = data[PROVIDER_ID_KEY]
        ds_row_id = data[ENTRY_KEY][DS_ROW_ID_KEY]
        form_code = data[ENTRY_KEY][FORM_CODE_KEY]

        self._raw_data = data

        self.attachment_bucket_base_path = (
            f"{IMAGE_STORE_PATH}/{provider_id}/{form_code}/{ds_row_id}"
        )

        self.attachment_download_base_url = (
            f"{IMAGE_DOWNLOAD_BASE_URL}/{provider_id}-{ds_row_id}"
        )

        self.attachments = self._find_attachments(
            data[ENTRY_KEY][ANSWERS_PAGES_KEY]
        )

    def get(self, var_path: str):
        return get_from_path(self._raw_data, var_path)

    def to_compiled_data(self) -> dict:
        """
        Compiles data by updating attachment storage paths.
        This is done so that ArcGIS can find the attachments.
        """
        transformed_data = self.to_dict()

        # Updating the attachment location to the bucket attachment's bucket location.
        for attachment in self.attachments:
            transformed_data[
                ENTRY_KEY
            ][
                ANSWERS_PAGES_KEY
            ][
                attachment.category
            ][
                attachment.type
            ] = (
                f"https://storage.googleapis.com/storage/v1/b/{IMAGE_STORE_BUCKET}/"
                f"o/{quote_plus(attachment.bucket_path)}?alt=media"
            )

        return transformed_data

    def to_dict(self) -> dict:
        return copy.deepcopy(self._raw_data)

    def _find_attachments(self, survey_pages) -> list:
        attachments = []

        for survey_page_name, survey_page in survey_pages.items():
            for survey_field, survey_value in survey_page.items():
                if self._is_survey_value_attachment(survey_value):
                    attachment = Attachment(
                        survey_page_name,
                        survey_field,
                        f"{self.attachment_bucket_base_path}/{survey_field}_{survey_value}",
                        f"{self.attachment_download_base_url}{survey_value}"
                    )

                    attachments.append(attachment)

        return attachments

    def is_passing_rules(self, rules: list) -> (bool, Optional[str]):
        """
        Checks if this form is passing provided rules.

        :return: (True if passing rules., An alert message if passed)
        :rtype: (bool, str | None)
        """
        return is_passing_form_rules(self._raw_data, rules)

    def is_excluded(self) -> (bool, Optional[str]):
        """
        Checks if this form is flagged as excluded.

        :return: (True if flagged as excluded., An alert message if flagged)
        :rtype: (bool, str)
        """
        return is_passing_exclude_rules(self._raw_data)

    @staticmethod
    def from_blob(blob: Blob):
        # Check if blob is man-made folder (0 byte object)
        if blob.size:
            json_data = blob.download_as_text()
            logging.info(f"Loading blob: {blob.name}")

            try:
                form_data = json.loads(json_data)
                form = Form(form_data)
            except (KeyError, json.decoder.JSONDecodeError) as exception:
                logging.error(
                    f"Invalid form: {blob.name}\n"
                    f"Exception: {str(exception)}"
                )
            else:
                # Checking if form flagged as excluded.
                excluded, alert = form.is_excluded()
                if excluded:
                    logging.info(str(alert))
                else:
                    return form
        else:
            logging.info(f"Blob '{blob.name}' is a zero-byte object (folder?), skipping...")

        return None

    @staticmethod
    def _is_survey_value_attachment(value) -> bool:
        if isinstance(value, str):
            name, extension = path.splitext(value)
            if extension:
                return extension in IMAGE_FILE_EXTENSIONS

        return False
