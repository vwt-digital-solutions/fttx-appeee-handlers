import base64
import json
import logging
import tempfile

from form_entry_service import FormEntryService
from gis_service import GISService
from storage_service import StorageService

logging.getLogger().setLevel(logging.INFO)


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
    feature_id = gis_service.add_object_to_feature_layer(gis_object)

    temp_file = tempfile.NamedTemporaryFile(mode="w+b", delete=True)

    storage_service = StorageService()
    file_type, file_name = storage_service.get_image(attachment_uri, temp_file.name)

    if file_type:
        attachment_id = gis_service.upload_attachment_to_feature_layer(
            feature_id, file_type, file_name, temp_file.read()
        )
        temp_file.close()

        gis_service.add_attachment_to_feature_layer(
            gis_object, feature_id, attachment_id
        )

    return "No Content", 204
