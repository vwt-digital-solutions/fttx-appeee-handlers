import json
import logging
import os
import re
import urllib

from config import (COORDINATE_SERVICE, COORDINATE_SERVICE_AUTHENTICATION,
                    COORDINATE_SERVICE_LATLON)
from requests_retry_session import get_requests_session
from utils import get_secret


class CoordinateService:
    def __init__(self):
        self.requests_session = get_requests_session(
            retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
        )
        self.token = self._get_feature_service_token()

    def _get_feature_service_token(self):
        data = {
            "f": "json",
            "username": COORDINATE_SERVICE_AUTHENTICATION["username"],
            "password": get_secret(
                os.environ["PROJECT_ID"], COORDINATE_SERVICE_AUTHENTICATION["secret"]
            ),
            "request": COORDINATE_SERVICE_AUTHENTICATION["request"],
            "referer": COORDINATE_SERVICE_AUTHENTICATION["referer"],
        }

        data = self.requests_session.post(
            COORDINATE_SERVICE_AUTHENTICATION["url"], data
        ).json()

        return data["token"]

    def download_coordinates_for_form_entry(self, form_entry):
        # Split incoming FCA_SLEUTEL: "1234AB56_78" -> ["1234AB56", "78"]
        address = form_entry["Entry"]["AnswersJson"]["SCHOUW_GEGEVENS_PAGE"][
            "FCA_SLEUTEL"
        ].split("_")
        address_postcode = address[0]
        address_ext = address[1] if len(address) > 1 else None

        # Regex postcode and housenumber (1234AB56)
        post_code = re.findall(r"^([0-9]{4}[a-zA-Z]{2})", address_postcode)[
            0
        ]  # [1234AB]56
        house_number = re.findall(
            r"^([0-9]+)", address_postcode.replace(post_code, "")
        )[
            0
        ]  # 1234AB[56]

        query_string = f"postcode='{post_code}' AND huisnummer='{house_number}'"

        # Query on huisext if available (78)
        if address_ext:
            query_string = f"{query_string} AND huisext='{address_ext}'"
        else:
            query_string = f"{query_string} AND huisext IS NULL"

        logging.info(f"Querying: {query_string}")

        url_query_string = urllib.parse.urlencode(
            {
                "where": query_string,
                "outFields": ",".join(COORDINATE_SERVICE_LATLON),
                "f": "json",
                "token": self.token,
            }
        )

        url = f"{COORDINATE_SERVICE}/query?{url_query_string}"

        data = self.requests_session.get(url).json()

        geometry = {}
        features = data.get("features", [])
        if len(features) > 0:
            geometry = features[0]["attributes"]
        elif "features" not in data:
            logging.error(
                f"Error occured when downloading coordinates. Request not successful: {json.dumps(data)}"
            )
        else:
            logging.error(
                "Error occured when downloading coordinates. Feature not found on feature server."
            )

        geo_json = self.convert_form_entry_to_geojson(form_entry, geometry)

        return geo_json

    @staticmethod
    def convert_form_entry_to_geojson(form_entry, attributes):
        latitude = float(attributes.get(COORDINATE_SERVICE_LATLON[0], 0))
        longitude = float(attributes.get(COORDINATE_SERVICE_LATLON[1], 0))

        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [latitude, longitude],
                    },
                    "properties": form_entry,
                }
            ],
        }
