import json

from config import COORDINATE_SERVICE_KEYFIELD
from form_object import Form
from gobits import Gobits
from google.cloud.pubsub_v1 import PublisherClient

from coordinate_service import CoordinateService
from requests_retry_session import get_requests_session
from retry import retry


class PublishService:
    def __init__(self, topic_name, context):
        self.requests_session = get_requests_session(
            retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
        )
        self._publisher = PublisherClient()
        self._topic_name = topic_name
        self.context = context
        self.coordinate_service = CoordinateService()

    @retry(tries=5, delay=5, backoff=2, logger=None)
    def publish_form(self, form: Form):
        """
        Publishes a form object to topic.

        NOTE: 'odh-arcgis-int' is configured to update 'Features' (in this case the transformed form data)
        when the 'entity_id' is equal to an existing Feature's 'entity_id'. In this case it's configured
        to be based on this form's postcode and huisnummer. This *should* make sure that no duplicate
        Features are create on the ArcGIS FeatureMap.
        """
        # TODO: Make sure no duplicate bops are created.

        data = form.to_compiled_data()

        # Converting/downloading the coordinates for this form.
        data = self.coordinate_service.download_coordinates_for_form_entry(
            data, COORDINATE_SERVICE_KEYFIELD
        )

        gobits = Gobits.from_context(context=self.context)
        message_to_publish = {"gobits": [gobits.to_json()], "appee_survey": data}
        self._publisher.publish(
            self._topic_name, bytes(json.dumps(message_to_publish).encode("utf-8"))
        )