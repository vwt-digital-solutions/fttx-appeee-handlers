import logging
import os
import re
from json.decoder import JSONDecodeError
from urllib.parse import urlencode

from config import (COORDINATE_SERVICE, COORDINATE_SERVICE_AUTHENTICATION,
                    COORDINATE_SERVICE_LATLON)
from requests.exceptions import ConnectionError, HTTPError
from requests_retry_session import get_requests_session
from retry import retry
from utils import get_secret


class CoordinateService:
    def __init__(self):
        self.requests_session = get_requests_session(
            retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
        )
        self.token = self._get_feature_service_token()

    def _get_feature_service_token(self):
        """
        Request a new feature service token

        :return: Token
        :rtype: str
        """

        try:
            return self.get_arcgis_token()
        except KeyError as e:
            logging.error(
                f"Function is missing authentication configuration for retrieving ArcGIS token: {str(e)}"
            )
            return None
        except (ConnectionError, HTTPError, JSONDecodeError) as e:
            logging.error(f"An error occurred when retrieving ArcGIS token: {str(e)}")
            return None

    @retry(
        (ConnectionError, HTTPError, JSONDecodeError),
        tries=3,
        delay=5,
        logger=None,
        backoff=2,
    )
    def get_arcgis_token(self):
        """
        Get token from ArcGIS

        :return: Token
        :rtype: str
        """

        request_data = {
            "f": "json",
            "username": COORDINATE_SERVICE_AUTHENTICATION["username"],
            "password": get_secret(
                os.environ["PROJECT_ID"], COORDINATE_SERVICE_AUTHENTICATION["secret"]
            ),
            "request": COORDINATE_SERVICE_AUTHENTICATION["request"],
            "referer": COORDINATE_SERVICE_AUTHENTICATION["referer"],
        }

        gis_r = self.requests_session.post(
            COORDINATE_SERVICE_AUTHENTICATION["url"], request_data
        )
        gis_r.raise_for_status()

        r_json = gis_r.json()

        if "token" in r_json:
            return r_json["token"]

        logging.error(
            f"An error occurred when retrieving ArcGIS token: {r_json.get('error', gis_r.content)}"
        )
        return None

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

        url_query_string = urlencode(
            {
                "where": query_string,
                "outFields": ",".join(COORDINATE_SERVICE_LATLON),
                "f": "json",
                "token": self.token,
            }
        )

        geometry = {}

        try:
            data = self.query_feature_layer(url_query_string)
        except (ConnectionError, HTTPError, JSONDecodeError) as e:
            logging.error(f"Error occurred when downloading coordinates: {str(e)}")
        else:
            if len(data.get("features", [])) > 0:
                geometry = data["features"][0]["attributes"]
            else:
                logging.info("Feature not found on feature server, skipping this.")

        geo_json = self.convert_form_entry_to_geojson(form_entry, geometry)

        return geo_json

    @retry(
        (ConnectionError, HTTPError, JSONDecodeError),
        tries=3,
        delay=5,
        logger=None,
        backoff=2,
    )
    def query_feature_layer(self, url_query_string):
        url = f"{COORDINATE_SERVICE}/query?{url_query_string}"
        data = self.requests_session.get(url).json()

        return data

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
                        "coordinates": [longitude, latitude],
                    },
                    "properties": form_entry,
                }
            ],
        }
