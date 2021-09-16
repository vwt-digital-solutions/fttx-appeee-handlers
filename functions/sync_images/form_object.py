import copy

from config import (
    IMAGE_STORE_PATH,
    IMAGE_DOWNLOAD_BASE_URL,
    IMAGE_STORE_BUCKET
)
from constant import (
    PROVIDER_ID_KEY,
    ENTRY_KEY,
    DS_ROW_ID_KEY,
    FORM_CODE_KEY,
    ANSWERS_PAGES_KEY,
    IMAGE_FILE_EXTENSIONS
)

from os import path
from urllib.parse import quote_plus


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

    def to_compiled_data(self):
        """
        Compiles data by updating attachment storage paths.
        This is done so that ArcGIS can find the attachments.
        """
        transformed_data = copy.deepcopy(self._raw_data)

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

    def _find_attachments(self, survey_pages):
        attachments = []

        for survey_page_name, survey_page in survey_pages.items():
            for survey_field, survey_value in survey_page.items():
                if _is_survey_value_attachment(survey_value):
                    attachment = Attachment(
                        survey_page_name,
                        survey_field,
                        f"{self.attachment_bucket_base_path}{survey_value}",
                        f"{self.attachment_download_base_url}/{survey_field}_{survey_value}"
                    )

                    attachments.append(attachment)

        return attachments


def _is_survey_value_attachment(value):
    return isinstance(value, str) and path.splitext(value)[1] in IMAGE_FILE_EXTENSIONS
