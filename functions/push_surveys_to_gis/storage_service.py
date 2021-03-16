import logging
import re

from google.api_core import exceptions as gcp_exceptions
from google.cloud import storage


class StorageService:
    def __init__(self):
        self.stg_client = storage.Client()

    def get_image(self, image_uri):
        # Get bucket and file name from uri (gs://[BUCKET_NAME]/[FILE_NAME])
        gcs_uri_match = re.match(r"^gs://([\w\d-_]*)/(.*)$", image_uri)
        bucket_name = gcs_uri_match.group(1)
        file_name = gcs_uri_match.group(2)

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
            blob_bytes = blob.download_as_bytes()

            return blob_bytes

        logging.error(f"File '{image_uri}' cannot be found, skipping upload")
        return None
