import json
import logging

from config import (
    IMAGE_STORE_BUCKET,
    TOPIC_NAME,
    ENTRY_FILEPATH_PREFIX
)

from attachment_service import AttachmentService
from form_object import Form
from google.cloud import storage
from publish_service import PublishService
from requests_retry_session import get_requests_session

logging.basicConfig(level=logging.INFO)


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

    # Skips download of attachments.
    skip_download = arguments.get("skip_download", False)

    # Suffix of the form bucket path.
    form_storage_suffix = arguments.get("form_storage_suffix", "")

    # Update arcgis entry even when no attachment was downloaded.
    force_arcgis_update = arguments.get("force_arcgis_update", False)

    # Retry options
    request_retry_options = arguments.get("request_retry_options", {})

    # fca_project # DEBUG
    debug_fca_project = arguments.get("debug_fca_project", None)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(IMAGE_STORE_BUCKET)

    attachment_service = AttachmentService(storage_client, get_requests_session(**request_retry_options))
    publish_service = PublishService(TOPIC_NAME, get_requests_session(**request_retry_options))

    # Getting all form blobs
    form_blobs = bucket.list_blobs(prefix=ENTRY_FILEPATH_PREFIX + form_storage_suffix)

    result = {
        "total_form_count": 0,
        "form_with_missing_attachment_count": 0,
        "missing_attachment_count": 0,
        "downloaded_attachment_count": 0
    }

    # Looping through all forms to check them.
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

        # Find all a form's attachments that are not available in storage.
        missing_attachments = attachment_service.find_missing_attachments(form)

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

        if (missing_attachments and not skip_download) or force_arcgis_update:
            # Sets a debug value to check if ArcGIS is receiving updates.
            # TODO: Remove when all tests have been completed.
            if debug_fca_project:
                form.set_debug_project(debug_fca_project)
            logging.info("Sending form to ArcGIS...")

            # Sending the form to ArcGIS
            publish_service.publish_form(form)

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
    if not request:
        return arguments

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
