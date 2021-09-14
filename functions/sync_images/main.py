import config
import json
import logging

from form_object import Form
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def handler(request):
    # Initializing components
    logging.info("Initializing storage.")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(config.FORM_STORE_BUCKET)

    # Getting all form blobs
    logging.info("Retrieving forms from bucket.")
    form_blobs = bucket.list_blobs(prefix=config.FORM_STORE_PATH)

    # Scanning forms
    for form_blob in form_blobs:
        form_data = json.loads(form_blob.download_as_string())

        try:
            form = Form(form_data)
        except KeyError as exception:
            logging.error(
                f"Invalid form: {form_blob.name}\n"
                f"Exception: {str(exception)}"
            )
            continue

        has_update = False

        logging.info(f"Found {len(form.attachments)} attachments in {form_blob.name}.")

        for attachment in form.attachments:
            if attachment.exists_in_bucket(bucket):
                continue

            success, response = attachment.download_to_bucket(bucket)

            if not success:
                logging.error(
                    "Error downloading image.\n"
                    f"URL: {attachment.download_url}\n"
                    f"Bucket path: {attachment.bucket_path}\n"
                    f"Response: {response}"
                )
            else:
                has_update = True

        if has_update:
            logging.info(f"Found missing attachments in {form_blob.name}.")
            form.send_to_arcgis()
        else:
            logging.info("No missing attachments found.")


if __name__ == "__main__":
    request = None
    handler(request)
