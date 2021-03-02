import json
import logging
import base64

from form_entry_service import FormEntryService
from gis_service import GISService


def handler(request):
    try:
        envelope = json.loads(request.data.decode('utf-8'))
        logging.info(envelope)
        _bytes = base64.b64decode(envelope['message']['data'])
        _message = json.loads(_bytes)
    except Exception as e:
        logging.exception('Failed while extracting message!')
        raise e

    form_entry_service = FormEntryService()
    gis_object = form_entry_service.convert_form_entry_to_gis_object(_message['appee_survey'])

    gis_service = GISService()
    gis_service.add_object_to_feature_layer(gis_object)
    return
