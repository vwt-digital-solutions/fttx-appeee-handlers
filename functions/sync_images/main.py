import json
import logging

from config import (
    IMAGE_STORE_BUCKET,
    TOPIC_NAME,
    ENTRY_FILEPATH_PREFIX
)

from functions.common.attachment_service import AttachmentService
from functions.common.form_object import Form
from functions.common.publish_service import PublishService
from functions.common.requests_retry_session import get_requests_session
from gobits import Gobits
from google.cloud import storage


logging.basicConfig(level=logging.INFO)

REQUEST_RETRY_SESSION = get_requests_session()


def handler(request):
    """
    This cloud function will attempt to correct ArcGIS features
    when needed. The repair mainly focuses on checking the attachments,
    and download them when missing. When sending the form to
    the ArcGIS interface it also causes the interface to remap all fields.
    This is desirable since it updates the ArcGIS features to their
    desired state.

    :param request: The request to this cloud function.
    :type request: flask.Request

    :return: The result of this cloud function., An HTTP status code.
    :rtype: str, int
    """

    # Initializing components
    arguments = get_request_arguments(request)

    # Can be used to specify a sub directory.
    form_storage_suffix = arguments.get("form_storage_suffix", "")

    # Download missing attachments.
    download_attachment_enabled = arguments.get("download_attachment_enabled", True)

    # Send entries to ArcGIS when changed.
    arcgis_update_enabled = arguments.get("arcgis_update_enabled", True)

    # Always send entries to ArcGIS.
    arcgis_update_forced = arguments.get("arcgis_update_forced", False)

    # Options for request retry.
    request_retry_options = arguments.get("request_retry_options", {
        "retries": 6,
        "backoff": 10,
        "status_forcelist": [
            404, 500, 502, 503, 504
        ]
    })

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(IMAGE_STORE_BUCKET)

    attachment_service = AttachmentService(storage_client, **request_retry_options)
    publish_service = PublishService(TOPIC_NAME, **request_retry_options)

    # Getting all form blobs
    form_blobs = bucket.list_blobs(prefix=ENTRY_FILEPATH_PREFIX + form_storage_suffix)
    form_blobs = list(form_blobs)

    result = {
        "total_form_count": 0,
        "form_with_missing_attachment_count": 0,
        "missing_attachment_count": 0,
        "downloaded_attachment_count": 0
    }

    logging.info(f"Getting all blobs from: {ENTRY_FILEPATH_PREFIX + form_storage_suffix}")
    logging.info(f"Found blobs: {str(len(form_blobs))}")

    # Looping through all forms to check them.
    for form_blob in form_blobs:
        result["total_form_count"] += 1
        json_data = form_blob.download_as_text()
        logging.info(f"JSON of blob({form_blob.name}): {json_data}")

        try:
            form_data = json.loads(json_data)
            form = Form(form_data)
        except (KeyError, json.decoder.JSONDecodeError) as exception:
            logging.error(
                f"Invalid form: {form_blob.name}\n"
                f"Exception: {str(exception)}"
            )
            continue

        # Find all a form's attachments that are not available in storage.
        missing_attachments = attachment_service.find_missing_attachments(form)

        if missing_attachments:
            missing_attachment_count = len(missing_attachments)
            result["form_with_missing_attachment_count"] += 1
            result["missing_attachment_count"] += missing_attachment_count

            if download_attachment_enabled:
                logging.info(
                    f"Found {missing_attachment_count} missing attachments for {form_blob.name}, "
                    "attempting to download..."
                )

                for attachment in missing_attachments:
                    # Downloading attachment to storage.
                    success, response = attachment_service.download(attachment)
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

        downloaded_missing_attachments = missing_attachments and download_attachment_enabled
        if (downloaded_missing_attachments and arcgis_update_enabled) or arcgis_update_forced:
            logging.info("Sending form to ArcGIS...")

            # Sending the form to ArcGIS
            gobits = Gobits.from_request(request=request)
            publish_service.publish_form(form, metadata=gobits)

    return json.dumps(result), 200


def get_request_arguments(request):
    """
    Extracts all arguments from HTTP arguments and JSON body.

    :param request: The request object to extract from.
    :type request: flask.Request

    :return: A dictionary of all arguments.
    :rtype: dict
    """

    arguments = dict()
    if request:
        json_body = request.get_json(silent=True)
        http_arguments = request.args

        for key, value in json_body.items():
            arguments[key] = value

        for key, value in http_arguments.items():
            arguments[key] = value

    return arguments


if __name__ == "__main__":
    request = None
    handler(request)
