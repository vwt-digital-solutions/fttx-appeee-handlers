import config
import json
import logging

from form_object import Form
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def handler(request):
    # Initializing components
    arguments = get_request_arguments(request)

    # Skips download of attachments.
    skip_download = arguments.get("skip_download", False)

    # Suffix of the form bucket path.
    form_storage_suffix = arguments.get("form_storage_suffix", "")

    # Update arcgis entry even when no attachment was downloaded.
    force_arcgis_update = arguments.get("force_arcgis_update", False)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(config.IMAGE_STORE_BUCKET)

    # Getting all form blobs
    form_blobs = bucket.list_blobs(prefix=config.ENTRY_FILEPATH_PREFIX + form_storage_suffix)

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

        if missing_attachments:
            missing_attachment_count = len(missing_attachments)
            result["form_with_missing_attachment_count"] += 1
            result["missing_attachment_count"] += missing_attachment_count

            if not skip_download:
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

        if missing_attachments or force_arcgis_update:
            logging.info("Sending form to ArcGIS...")
            form.send_to_arcgis()

    return json.dumps(result), 200


def get_request_arguments(request):
    arguments = dict()
    if not request:
        return arguments

    json_body = request.get_json(silent=True)
    http_arguments = request.args

    for key, value in json_body.items():
        arguments[key] = value

    for key, value in http_arguments.items():
        arguments[key] = value


if __name__ == "__main__":
    request = None
    handler(request)
