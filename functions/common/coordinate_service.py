import logging
import os
import re
from json.decoder import JSONDecodeError
from urllib.parse import urlencode

from config import (
    COORDINATE_SERVICE,
    COORDINATE_SERVICE_AUTHENTICATION,
    COORDINATE_SERVICE_LATLON,
    COORDINATE_SERVICE_KEYFIELD,
    COORDINATE_SERVICE_KEYFIELD_FALLBACK  # Disgusting
)

from requests.exceptions import ConnectionError, HTTPError
from requests_retry_session import get_requests_session
from utils import get_secret, get_from_path
from form_object import Form
from typing import Optional


class CoordinateService:
    def __init__(self, **kwargs):
        self.requests_session = get_requests_session(**kwargs)
        self.token = self._request_authentication_token()

    def form_to_geojson(self, form: Form) -> Optional[dict]:
        address = self._extract_form_address(form)
        latitude, longitude = self._find_house_coordinates(**address)

        if latitude is None or longitude is None:
            return None

        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [longitude, latitude],
                    },
                    "properties": form.to_compiled_data(),
                }
            ],
        }

    def _request_authentication_token(self) -> str:
        """
        Requests a new token for ArcGIS authentication.

        :return: A new token for ArcGIS authentication.
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

        try:
            result = self.requests_session.post(
                COORDINATE_SERVICE_AUTHENTICATION["url"], request_data
            )

            result.raise_for_status()
            data = result.json()

        except (ConnectionError, HTTPError) as exception:
            logging.error(f"Could not reach ArcGIS: {str(exception)}")
        except JSONDecodeError as exception:
            logging.error(f"ArcGIS did not respond with valid JSON: {str(exception)}")
        else:
            if data and "token" in data:
                return data["token"]
            else:
                logging.error("ArcGIS did not respond with token.")

        raise LookupError("Could not get token from ArcGIS")

    def _find_house_coordinates(self, zip_code: str, house_number: int, suffix=None) -> (float, float):
        """
        Makes a query to ArcGIS to get the latitude and longitude of the specified address.

        :param zip_code: The zip code or postcode of the address.
        :type zip_code: str
        :param house_number: The house number of the address.
        :type house_number: int
        :param suffix: The suffix of the address (could be a letter, apartment number, etc.)
        :type suffix: str

        :return: (
            Latitude of the specified address in epsg4326 format.,
            Longitude of the specified address in epsg4326 format
        )
        :rtype: float | None, float | None
        """

        query_string = f"postcode='{zip_code}' AND huisnummer='{house_number}'"

        # Query on suffix if available
        if suffix:
            query_string = f"{query_string} AND huisext='{suffix}'"
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

        success, result = self._query_feature_layer(url_query_string)

        if success:
            features = result.get("features", [])
            if features:
                # Get first (query should have returned only one result)
                feature = features[0]

                if "attributes" in feature:
                    attributes = feature["attributes"]
                    return (
                        float(attributes.get(COORDINATE_SERVICE_LATLON[0], 0)),
                        float(attributes.get(COORDINATE_SERVICE_LATLON[1], 0))
                    )
                else:
                    logging.error("Could not find coordinates in feature")
            else:
                logging.info(f"Feature with postcode '{zip_code}' and huisnummer '{house_number}' not found.")
        else:
            logging.error(f"Error occurred when requesting feature layer: {str(result)}")

        return None, None

    @staticmethod
    def _extract_form_address(form: Form) -> dict:
        regex = r"^(\d{4}[A-Z]{2})(\d+)(?:_(.+))?$"
        key = get_from_path(form.to_dict(), COORDINATE_SERVICE_KEYFIELD)
        
        if not key:
            key = get_from_path(form.to_dict(), COORDINATE_SERVICE_KEYFIELD_FALLBACK)

        result = re.search(regex, key)

        return {
            "zip_code": result.group(1),
            "house_number": result.group(2),
            "suffix": result.group(3)
        }

    def _query_feature_layer(self, url_query_string):
        try:
            url = f"{COORDINATE_SERVICE}/query?{url_query_string}"
            data = self.requests_session.get(url).json()
        except (ConnectionError, HTTPError, JSONDecodeError) as exception:
            return False, str(exception)
        else:
            return True, data
