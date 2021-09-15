import config
import json
import logging

from form_object import Form
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def handler(request):
    # Initializing components
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(config.FORM_STORE_BUCKET)

    # Getting all form blobs
    form_blobs = bucket.list_blobs(prefix=config.FORM_STORE_PATH)

    result = {
        "total_form_count": 0,
        "form_with_missing_attachment_count": 0,
        "missing_attachment_count": 0,
        "downloaded_attachment_count": 0
    }

    # Scanning forms
    for form_blob in form_blobs:
        result["total_form_count"] += 1
        form_data = json.loads(form_blob.download_as_string())

        try:
            form = Form(form_data)
        except KeyError as exception:
            logging.error(
                f"Invalid form: {form_blob.name}\n"
                f"Exception: {str(exception)}"
            )
            continue

        missing_attachments = [att for att in form.attachments if not att.exists_in_bucket(bucket)]

        if not missing_attachments:
            continue

        missing_attachment_count = len(missing_attachments)
        result["form_with_missing_attachment_count"] += 1
        result["missing_attachment_count"] += missing_attachment_count

        logging.info(
            f"Found {missing_attachment_count} missing attachments for {form_blob.name}, "
            "attempting to download..."
        )

        for attachment in missing_attachments:
            success, response = attachment.download_to_bucket(bucket)
            if success:
                result["downloaded_attachment_count"] += 1
            else:
                logging.error(
                    "Error downloading image.\n"
                    f"Form: {form_blob.name}\n"
                    f"URL: {attachment.download_url}\n"
                    f"Bucket path: {attachment.bucket_path}\n"
                    f"Response: {response}"
                )

        logging.info("Download(s) complete.")

    return json.dumps(result), 200


if __name__ == "__main__":
    request = None
    handler(request)
