import base64
import json
import logging

from form_entry_service import FormEntryService
from gis_service import GISService
from storage_service import StorageService


def handler(request):
    try:
        envelope = json.loads(request.data.decode("utf-8"))
        logging.info(envelope)
        _bytes = base64.b64decode(envelope["message"]["data"])
        _message = json.loads(_bytes)
    except Exception as e:
        logging.exception("Failed while extracting message!")
        raise e

    form_entry_service = FormEntryService()
    gis_object, attachment_uri = form_entry_service.convert_form_entry_to_gis_object(
        _message["appee_survey"]
    )

    gis_service = GISService()
    new_feature = gis_service.add_object_to_feature_layer(gis_object)

    storage_service = StorageService()
    attachment_file = storage_service.get_image(attachment_uri)

    if attachment_file:
        attachment_id = gis_service.upload_attachment_to_feature_layer(
            new_feature["id"], attachment_file
        )
        gis_service.add_attachment_to_feature_layer(new_feature, attachment_id)

    return
