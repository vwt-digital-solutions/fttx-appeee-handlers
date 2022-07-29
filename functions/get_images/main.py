import logging


from config import (
    ENTRY_FILEPATH_PREFIX,
    TOPIC_NAME_FALLBACK
)

from functions.common.form_object import Form
from functions.common.attachment_service import AttachmentService
from functions.common.publish_service import PublishService
from gobits import Gobits
from google.cloud import storage

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

    if not filename.startswith(ENTRY_FILEPATH_PREFIX):
        logging.info(
            f"Skip file gs://{bucket_name}/{filename}: File is not in {ENTRY_FILEPATH_PREFIX}"
        )
        return

    logging.info(
        f"Processing APPEEE form input from file gs://{bucket_name}/{filename}"
    )

    # Retrieve form entry
    storage_client = storage.Client()
    entry_bucket = storage_client.get_bucket(bucket_name)
    entry_blob = entry_bucket.get_blob(filename)
    form = Form.from_blob(entry_blob)

    if not form:
        logging.warning(f"Could not get form object from {entry_blob.name}")
        return

    # Setup services
    attachment_service = AttachmentService(storage_client)
    publish_service = PublishService(TOPIC_NAME_FALLBACK)

    # Download images
    logging.info("Downloading images")
    for attachment in form.attachments:
        if attachment_service.exists(attachment):
            logging.warning(
                f"Image '{attachment.bucket_path}' already exists, skipping.")
        else:
            success, response = attachment_service.download(attachment)
            if not success:
                logging.error(
                    "Error downloading image.\n"
                    f"Form: {entry_blob.name}\n"
                    f"URL: {attachment.download_url}\n"
                    f"Bucket path: {attachment.bucket_path}\n"
                    f"Response: {response}"
                )

    # Publish form to topic
    gobits = Gobits.from_context(context=context)
    publish_service.publish_form(form, gobits)


if __name__ == "__main__":
    context = None
    data = dict(bucket="", name="")
    handler(data, context)
