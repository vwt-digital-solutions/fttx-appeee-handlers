import json
import logging
import re

from config import (
    IMAGE_STORE_BUCKET,
    TOPIC_NAME,
    ENTRY_FILEPATH_PREFIX
)

from datetime import datetime, timedelta, timezone
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
    publish_service = PublishService(TOPIC_NAME, **request_retry_options)

    # Getting all form blobs
    form_blobs = []
    for suffix in unpack_ranges(form_storage_suffix):
        form_blobs.extend(storage_client.list_blobs(
            bucket_or_name=IMAGE_STORE_BUCKET,
            prefix=ENTRY_FILEPATH_PREFIX + suffix
        ))

    if form_index_range:
        start, end = form_index_range.split(":")
        form_blobs = form_blobs[int(start):int(end)]

    result = {
        "total_form_count": 0,
        "form_with_missing_attachment_count": 0,
        "missing_attachment_count": 0,
        "downloaded_attachment_count": 0
    }

    logging.info(f"Getting all blobs from: {ENTRY_FILEPATH_PREFIX + form_storage_suffix}")
    logging.info(f"Found blobs: {len(form_blobs)}")

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


def unpack_ranges(pattern) -> list:
    """
    Unpacks all possible range combinations.

    Range syntax: [{start}-{end}]
        start: start of the range (included)
        end: end of the range (excluded)
    Range example: [1-10]

    While unpacking the ranges will be formatted to stringified numbers.
    There numbers will be justified based on min(len({start}), len({end}))).

    Example 1: '[8-11]'
    Result 1: ['8', '9', '10']

    Example 2: '[08-11]'
    Result 2: ['08', '09', '10']

    Example 3: 'A:[1-3] B:[1-3]'
    Result 3: ['A:1 B:1', 'A:1 B:2', 'A:2 B:1', 'A:2 B:2']

    :param pattern: The pattern to unpack.
    :type pattern: str:

    :return: A list of all possible range combinations.
    :rtype: list[str]
    """
    range_regex = r"\[(\d+)-(\d+)]"
    match = re.search(range_regex, pattern)

    suffixes = []
    if match:
        start = match.group(1)
        end = match.group(2)
        justified = min(len(start), len(end))

        for i in range(int(start), int(end)):
            prefix = pattern[:match.start(0)]
            suffix = pattern[match.end(0):]
            number = str(i).rjust(justified, "0")
            string = f"{prefix}{number}{suffix}"
            suffixes.extend(unpack_ranges(string))
    else:
        suffixes.append(pattern)

    return suffixes


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
