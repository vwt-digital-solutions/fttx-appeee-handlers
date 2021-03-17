import json
import logging
import os

from config import GIS_FEATURE_SERVICE, GIS_FEATURE_SERVICE_AUTHENTICATION
from requests_retry_session import get_requests_session
from utils import get_secret


class GISService:
    def __init__(self):
        self.requests_session = get_requests_session(
            retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
        )
        self.token = self._get_feature_service_token()

    def _get_feature_service_token(self):
        data = {
            "f": "json",
            "username": GIS_FEATURE_SERVICE_AUTHENTICATION["username"],
            "password": get_secret(
                os.environ["PROJECT_ID"], GIS_FEATURE_SERVICE_AUTHENTICATION["secret"]
            ),
            "request": GIS_FEATURE_SERVICE_AUTHENTICATION["request"],
            "referer": GIS_FEATURE_SERVICE_AUTHENTICATION["referer"],
        }

        data = self.requests_session.post(
            GIS_FEATURE_SERVICE_AUTHENTICATION["url"], data
        ).json()
        return data["token"]

    def add_object_to_feature_layer(self, gis_object):
        data = {"adds": json.dumps([gis_object]), "f": "json", "token": self.token}

        r = self.requests_session.post(f"{GIS_FEATURE_SERVICE}/applyEdits", data=data)

        try:
            response = r.json()
            if response.get("error", False):
                raise Exception(
                    f"Error when adding feature to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )

            feature_id = response["addResults"][0]["objectId"]
            logging.info(f"Added new feature to map with ID {feature_id}")

            return feature_id
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Status-code: {r.status_code}")
            logging.error(f"Output:\n{r.text}")
            logging.exception(e)

    def upload_attachment_to_feature_layer(
        self, feature_id, file_type, file_name, file_content
    ):
        data = {"f": "json", "token": self.token}

        files = [("attachment", (file_name, file_content, file_type))]

        r = self.requests_session.post(
            f"{GIS_FEATURE_SERVICE}/{feature_id}/addAttachment", data=data, files=files
        )

        try:
            response = r.json()
            if response.get("error", False):
                raise Exception(
                    f"Error when uploading attachment to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
            return response["addAttachmentResult"]["objectId"]
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Status-code: {r.status_code}")
            logging.error(f"Output:\n{r.text}")
            logging.exception(e)

    def add_attachment_to_feature_layer(self, feature, feature_id, attachment_id):
        feature["attributes"]["objectid"] = int(feature_id)
        feature["attributes"]["foto_bop_att_id"] = int(attachment_id)

        data = {"updates": json.dumps([feature]), "f": "json", "token": self.token}

        r = self.requests_session.post(f"{GIS_FEATURE_SERVICE}/applyEdits", data=data)

        try:
            response = r.json()
            if response.get("error", False):
                raise Exception(
                    f"Error when updating feature to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
            return
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Status-code: {r.status_code}")
            logging.error(f"Output:\n{r.text}")
            logging.exception(e)
