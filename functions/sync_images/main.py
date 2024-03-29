import re
import json
import logging

from config import (
    IMAGE_STORE_BUCKET,
    TOPIC_NAME_FALLBACK,
    ENTRY_FILEPATH_PREFIX
)

from datetime import datetime, timedelta, timezone
from functions.common.attachment_service import AttachmentService
from functions.common.form_object import Form
from functions.common.publish_service import PublishService
from functions.common.requests_retry_session import get_requests_session
from functions.common.utils import unpack_ranges, get_request_arguments
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

    # Range of indexes
    form_index_range = arguments.get("form_index_range")

    # Specifies the maximum age of the blobs, older blobs will be ignored.
    max_time_delta = timedelta(**arguments["max_time_delta"]) if "max_time_delta" in arguments else None

    # Download missing attachments.
    enable_attachment_downloading = arguments.get("enable_attachment_downloading", True)

    # Send entries to ArcGIS when changed.
    enable_arcgis_updating = arguments.get("enable_arcgis_updating", True)

    # Always send entries to ArcGIS.
    force_arcgis_updating = arguments.get("force_arcgis_updating", False)

    # Options for request retry.
    request_retry_options = arguments.get("request_retry_options", {
        "retries": 6,
        "backoff": 10,
        "status_forcelist": [
            404, 500, 502, 503, 504
        ]
    })

    # Get the current time for delta time calculations.
    process_start_time = datetime.now(timezone.utc)  # timestamp must be timezone aware and conform to RFC3339

    storage_client = storage.Client()
    attachment_service = AttachmentService(storage_client, **request_retry_options)
    publish_service = PublishService(TOPIC_NAME_FALLBACK, **request_retry_options)

    # Getting all form blobs
    form_blobs = []
    for suffix in unpack_ranges(form_storage_suffix):
        form_blobs.extend(storage_client.list_blobs(
            bucket_or_name=IMAGE_STORE_BUCKET,
            prefix=ENTRY_FILEPATH_PREFIX + suffix
        ))

    logging.info(f"Getting all blobs from: {ENTRY_FILEPATH_PREFIX + form_storage_suffix}")
    logging.info(f"Found blobs: {len(form_blobs)}")

    if form_index_range:
        match = re.match(r"^(\d+):(\d+)$", form_index_range)
        if match:
            start = match.group(1)
            end = match.group(2)
            form_blobs = form_blobs[int(start):int(end)]
            logging.info(f"Index range: start: {start} ({form_blobs[0].name}), end: {end} ({form_blobs[-1].name})")

    result = {
        "total_form_count": 0,
        "form_with_missing_attachment_count": 0,
        "missing_attachment_count": 0,
        "downloaded_attachment_count": 0
    }

    # Looping through all forms to check them.
    for form_blob in form_blobs:
        # Check if blob creation time does not exceed max age.
        if max_time_delta and process_start_time - form_blob.time_created > max_time_delta:
            continue

        form = Form.from_blob(form_blob)

        if not form:
            continue

        result["total_form_count"] += 1

        # Find all a form's attachments that are not available in storage.
        missing_attachments = attachment_service.find_missing_attachments(form)

        if missing_attachments:
            missing_attachment_count = len(missing_attachments)
            result["form_with_missing_attachment_count"] += 1
            result["missing_attachment_count"] += missing_attachment_count

            if enable_attachment_downloading:
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

        downloaded_missing_attachments = missing_attachments and enable_attachment_downloading
        if (downloaded_missing_attachments and enable_arcgis_updating) or force_arcgis_updating:
            logging.info("Sending form to ArcGIS...")

            # Sending the form to ArcGIS
            gobits = Gobits.from_request(request=request)
            publish_service.publish_form(form, metadata=gobits)

    return json.dumps(result), 200


if __name__ == "__main__":
    request = None
    handler(request)
