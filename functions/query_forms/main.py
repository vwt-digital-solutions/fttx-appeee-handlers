import json
import logging

from config import (
    IMAGE_STORE_BUCKET,
    ENTRY_FILEPATH_PREFIX
)

from google.cloud import storage

from functions.common.utils import get_request_arguments, unpack_ranges, get_from_path
from functions.common.form_rule import rule_alerts_from_dict, is_passing_rules
from functions.common.requests_retry_session import get_requests_session


logging.basicConfig(level=logging.INFO)

REQUEST_RETRY_SESSION = get_requests_session()


def handler(request):
    """
    :param request: The request to this cloud function.
    :type request: flask.Request

    :return: The result of this cloud function., An HTTP status code.
    :rtype: str, int
    """

    # Initializing components
    arguments = get_request_arguments(request)

    # Can be used to specify a sub directory.
    form_storage_suffix = arguments.get("form_storage_suffix", "")

    query_rules = rule_alerts_from_dict(arguments.get("query", []))

    output_format = arguments.get("output_format", {
        "blob_name": "$BLOB_NAME"
    })

    storage_client = storage.Client()

    # Getting all form blobs
    form_blobs = []
    for suffix in unpack_ranges(form_storage_suffix):
        form_blobs.extend(storage_client.list_blobs(
            bucket_or_name=IMAGE_STORE_BUCKET,
            prefix=ENTRY_FILEPATH_PREFIX + suffix
        ))

    results = {
        "matching_forms": []
    }

    logging.info(f"Scanning '{len(form_blobs)}' BLOBs.")

    for form_blob in form_blobs:
        if not form_blob.size:
            continue

        json_blob_data = form_blob.download_as_text()
        raw_form_data = json.loads(json_blob_data)

        success, alert = is_passing_rules(raw_form_data, query_rules)

        if success:
            output = output_format.copy()
            for key, value in output.items():
                if "$BLOB_NAME" in value:
                    output[key] = value.replace("$BLOB_NAME", form_blob.name)
                else:
                    output[key] = get_from_path(raw_form_data, value)
            logging.info(f"BLOB '{form_blob.name}' matched the query.")
            if alert:
                logging.info(str(alert))
            results["matching_forms"].append(output)

    return json.dumps(results), 200


if __name__ == "__main__":
    request = None
    handler(request)
