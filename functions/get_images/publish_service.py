import json
import logging

from retry import retry

from requests_retry_session import get_requests_session
from google.cloud.pubsub_v1 import PublisherClient


class PublishService:
    def __init__(self, topic_name):
        self.requests_session = get_requests_session(retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504))
        self._publisher = PublisherClient()
        self._topic_name = topic_name

    @retry(tries=5, delay=5, backoff=2, logger=None)
    def publish_message(self, form_entry):
        message_to_publish = {'gobits': [], 'appee_survey': form_entry}
        logging.info(f'Publishing message on topic {self._topic_name}')
        print(json.dumps(message_to_publish))
        self._publisher.publish(self._topic_name, bytes(json.dumps(message_to_publish).encode('utf-8')))
