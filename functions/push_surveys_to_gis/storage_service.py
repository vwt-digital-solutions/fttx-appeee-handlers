import logging
import re

from google.api_core import exceptions as gcp_exceptions
from google.cloud import storage


class StorageService:
    def __init__(self):
        self.stg_client = storage.Client()

    def get_image(self, image_uri, temp_file_name):
        """
        Get an image from Google Cloud Storage

        :param image_uri: Image URI
        :type image_uri: str
        :param temp_file_name: Temporary file name
        :type temp_file_name: str

        :return: Content-type, File name
        :rtype: (str, str)
        """

        # Get bucket and file name from uri (gs://[BUCKET_NAME]/[FILE_NAME])
        gcs_uri_match = re.match(r"^gs://([a-zA-Z0-9-_]*)/(.*)$", image_uri)
        bucket_name = gcs_uri_match.group(1)
        file_name = gcs_uri_match.group(2)
        file_name_clean = file_name.split("/")[-1]

        # Get bucket
        try:
            bucket = self.stg_client.get_bucket(bucket_name)
        except gcp_exceptions.NotFound:
            logging.error(f"Bucket '{bucket_name}' cannot be found, skipping upload")
            return None

        # Check if blob exist and download as bytes
        if storage.Blob(bucket=bucket, name=file_name).exists(self.stg_client):
            logging.info(f"Downloading file '{file_name}'")

            blob = bucket.blob(file_name)
            blob.download_to_filename(temp_file_name)

            return blob.content_type, file_name_clean

        logging.error(f"File '{image_uri}' cannot be found, skipping upload")
        return None, None
