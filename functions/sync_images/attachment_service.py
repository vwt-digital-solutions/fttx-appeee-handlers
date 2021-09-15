import requests

from config import IMAGE_STORE_BUCKET

from form_object import Attachment, Form
from mimetypes import guess_type


class AttachmentService:
    def __init__(self, storage_client, request_session):
        self.storage_client = storage_client
        self.requests_session = request_session
        self.bucket = storage_client.get_bucket(IMAGE_STORE_BUCKET)

    def exists(self, attachment: Attachment):
        """
        Checks for the existence of the specified attachment.

        :param attachment: The attachment to check.
        :type attachment: Attachment
        :return: `True` if attachment exists in the specified bucket, `False` otherwise.
        :rtype: bool
        """

        blob = self.bucket.get_blob(attachment.bucket_path)
        return blob and blob.exists()

    def find_missing_attachments(self, form: Form):
        return [att for att in form.attachments if not self.exists(att)]

    def download(self, attachment: Attachment):
        """
        Downloads the specified attachment to its bucket path.

        :param attachment: The attachment to download
        :type attachment: Attachment
        :return: `True` if the download was successful, `False` otherwise.,
            The response message.
        :rtype: int, str
        """

        try:
            file_response = self.requests_session.get(attachment.download_url)
        except (
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                requests.exceptions.RetryError,
        ) as exception:
            return False, str(exception)

        if file_response.status_code == requests.codes.ok:
            content_type, _ = guess_type(attachment.bucket_path)

            image_store_blob = self.bucket.blob(attachment.bucket_path)
            image_store_blob.upload_from_string(
                file_response.content, content_type=content_type
            )
        else:
            return False, file_response.text

        return True, f"{attachment.download_url} downloaded to {attachment.bucket_path}"
