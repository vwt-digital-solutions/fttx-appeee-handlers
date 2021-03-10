import json
import logging

import config
from coordinate_service import CoordinateService
from google.cloud import storage
from image_service import ImageService
from publish_service import PublishService

logging.basicConfig(level=logging.INFO)


def handler(data, context):
    """
    Handler method that processed form answers received on bucket. It downloads its images,
    updates its coordinates, and publishes it on a topic.

    :param: data    Dictionary like object that holds trigger information.
    :param: context Google Cloud Function context.
    """

    bucket_name = data["bucket"]
    filename = data["name"]

    if not filename.startswith(config.ENTRY_FILEPATH_PREFIX):
        logging.info(
            f"Skip file gs://{bucket_name}/{filename}: File is not in {config.ENTRY_FILEPATH_PREFIX}"
        )
        return

    logging.info(
        f"Processing APPEEE form input from file gs://{bucket_name}/{filename}"
    )

    # Retrieve form entry
    storage_client = storage.Client()
    entry_bucket = storage_client.get_bucket(bucket_name)
    entry_blob = entry_bucket.blob(filename)
    form_entry = json.loads(entry_blob.download_as_string())

    # Download images
    logging.info("Downloading images")
    form_entry = ImageService(storage_client).download_images_for_form_entry(form_entry)

    # Add coordinates and convert to geojson.
    coordinate_service = CoordinateService()
    geo_json = coordinate_service.download_coordinates_for_form_entry(form_entry)

    # Publish form entry
    publish_service = PublishService(config.TOPIC_NAME, context)
    publish_service.publish_message(geo_json)


if __name__ == "__main__":
    context = None
    data = dict(bucket="", name="")
    handler(data, context)
