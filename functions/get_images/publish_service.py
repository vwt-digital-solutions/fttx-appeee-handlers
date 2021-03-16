import json
import logging

from gobits import Gobits
from google.cloud.pubsub_v1 import PublisherClient
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

    @retry(tries=5, delay=5, backoff=2, logger=None)
    def publish_message(self, form_entry):
        gobits = Gobits.from_context(context=self.context)
        message_to_publish = {"gobits": [gobits], "appee_survey": form_entry}
        logging.info(f"Publishing message on topic {self._topic_name}")
        self._publisher.publish(
            self._topic_name, bytes(json.dumps(message_to_publish).encode("utf-8"))
        )
