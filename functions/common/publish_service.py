import json
import logging

from gobits import Gobits
from coordinate_service import CoordinateService
from form_object import Form
from google.cloud.pubsub_v1 import PublisherClient
from retry import retry
from form_rule import (
    TOPIC_ROUTE_RULES,
    is_passing_rule
)


class PublishService:
    def __init__(self, topic_name_fallback, **kwargs):
        self._publisher = PublisherClient()
        self._topic_name_fallback = topic_name_fallback
        self.coordinate_service = CoordinateService(**kwargs)

    @retry(tries=5, delay=5, backoff=2, logger=None)
    def publish_form(self, form: Form, metadata: Gobits):
        """
        Publishes a form object to topic.

        NOTE: 'odh-arcgis-int' is configured to update 'Features' (in this case the transformed form data)
        when the 'entity_id' is equal to an existing Feature's 'entity_id'. In this case it's configured
        to be based on this form's postcode and huisnummer. This *should* make sure that no duplicate
        Features are create on the ArcGIS FeatureMap.

        :param form: The form to publish to topic.
        :type form: Form
        :param metadata: Metadata of cloud function trigger event.
        :type metadata: Gobits
        """

        # Converting/downloading the coordinates for this form.
        data = self.coordinate_service.form_to_geojson(form)
        if data:
            # Publish message to topic to be picked up by the ArcGIS interface.
            message_to_publish = {
                "appee_survey": data,
                "gobits": [metadata.to_json()]
            }

            raw_form_data = form.to_dict()
            topic_name = self._topic_name_fallback
            for route_rule in TOPIC_ROUTE_RULES:
                if is_passing_rule(raw_form_data, route_rule):
                    topic_name = route_rule["data"]["topic_name"]

            logging.info(f"Publishing form to ArcGIS interface ({topic_name}).")

            future = self._publisher.publish(
                topic_name, bytes(json.dumps(
                    message_to_publish).encode("utf-8"))
            )

            logging.info(f"Published form to ArcGIS interface ({topic_name}) with ID {future.result()}")
        else:
            logging.error("Could not get data to send to ArcGIS, skipping...")
