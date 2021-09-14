import config
import constant

from os import path


class Attachment:
    def __init__(self, bucket_path, download_url):
        self.bucket_path = bucket_path
        self.download_url = download_url

    def exists_in_bucket(self, bucket):
        """
        Checks for the existence of this attachment in the specified bucket.

        :param bucket: The bucket to check.
        :type bucket: Bucket
        :return: `True` if attachment exists in the specified bucket, `False` otherwise.
        """

        blob = bucket.get_blob(self.bucket_path)
        return blob.exists()

    def download_to_bucket(self, bucket):
        """
        Downloads this attachment to the specified bucket.

        :param bucket: The bucket to download this attachment to.
        :type bucket: Bucket
        :return: `True` if the download was successful, `False` otherwise.,
            The response message.
        :rtype: int, str
        """

        # TODO: Do this later after testing.
        return True, "Download function not implemented yet."


class Form:
    def __init__(self, data):
        provider_id = data[constant.PROVIDER_ID_KEY]
        ds_row_id = data[constant.ENTRY_KEY][constant.DS_ROW_ID_KEY]
        form_code = data[constant.ENTRY_KEY][constant.FORM_CODE_KEY]

        self.attachment_bucket_base_path = (
            f"{config.IMAGE_STORE_PATH}/{provider_id}/{form_code}/{ds_row_id}"
        )

        self.attachment_download_base_url = (
            f"{config.IMAGE_DOWNLOAD_BASE_URL}/{provider_id}-{ds_row_id}"
        )

        self.attachments = self._find_attachments(
            data[constant.ENTRY_KEY][constant.ANSWERS_PAGES_KEY]
        )

    def send_to_arcgis(self):
        """
        Transforms the form data to fit the 'odh-arcgis-int' requirements,
        and sends this transformed data to the cloud topic.

        NOTE: 'odh-arcgis-int' is configured to update 'Features' (in this case the transformed form data)
        when the 'entity_id' is equal to an existing Feature's 'entity_id'. In this case it's configured
        to be based on this form's postcode and huisnummer. This *should* make sure that no duplicate
        Features are create on the ArcGIS FeatureMap.
        """
        # TODO: Make sure that no duplicate BOPs will be made.
        pass

    def _find_attachments(self, survey_pages):
        attachments = []

        for survey_page_name, survey_page in survey_pages.items():
            for survey_field, survey_value in survey_page.items():
                if _is_survey_value_attachment(survey_value):
                    attachment = Attachment(
                        f"{self.attachment_bucket_base_path}{survey_value}",
                        f"{self.attachment_download_base_url}/{survey_field}_{survey_value}"
                    )

                    attachments.append(attachment)

        return attachments


def _is_survey_value_attachment(value):
    return isinstance(value, str) and path.splitext(value)[1] in constant.IMAGE_FILE_EXTENSIONS
