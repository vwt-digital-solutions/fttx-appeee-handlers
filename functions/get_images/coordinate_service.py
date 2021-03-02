import logging
import os
import re
import urllib

from requests_retry_session import get_requests_session
from config import COORDINATE_SERVICE_AUTHENTICATION, COORDINATE_SERVICE
from utils import get_secret


class CoordinateService:

    def __init__(self):
        self.requests_session = get_requests_session(retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504))
        self.token = self._get_feature_service_token()

    def _get_feature_service_token(self):
        data = {
            'f': 'json',
            'username': COORDINATE_SERVICE_AUTHENTICATION['username'],
            'password': get_secret(os.environ['PROJECT_ID'], COORDINATE_SERVICE_AUTHENTICATION['secret']),
            'request': COORDINATE_SERVICE_AUTHENTICATION['request'],
            'referer': COORDINATE_SERVICE_AUTHENTICATION['referer'],
        }

        data = self.requests_session.post(COORDINATE_SERVICE_AUTHENTICATION['url'], data).json()
        return data['token']

    def download_coordinates_for_form_entry(self, form_entry):
        address_data = form_entry['Entry']['AnswersJson']['SCHOUW_GEGEVENS_PAGE']

        post_code = address_data['FCA_SLEUTEL'][:6]
        house_number = re.findall('\\d+', address_data['FCA_SLEUTEL'][6:])[0]
        try:
            house_number_ext = re.findall('[a-z,A-Z]+', address_data['FCA_SLEUTEL'][6:])[0]
        except IndexError:
            house_number_ext = ''

        address = address_data['ADRES']

        query_string = f"postcode='{post_code}' AND huisnummer='{house_number}'" \
                       f" AND huisnummer_ext='{house_number_ext}' AND adres='{address}'"

        url_parameters = {
            'where': query_string,
            'f': 'json',
            'token': self.token
        }
        url_query_string = urllib.parse.urlencode(url_parameters)

        url = f'{COORDINATE_SERVICE}/query?{url_query_string}'

        data = self.requests_session.get(url).json()

        features = data.get('features', [])
        if len(features) == 0:
            logging.error('Error occured when downloading coordinates. Feature not found on feature server.')
            geometry = {
                'x': 0,
                'y': 0
            }
        else:
            geometry = features[0]['geometry']

        geo_json = self.convert_form_entry_to_geojson(form_entry, geometry)

        return geo_json

    def convert_form_entry_to_geojson(self, form_entry, geometry):
        return {
            'type': 'FeatureCollection',
            'features': [{
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [geometry['x'], geometry['y']]
                },
                'properties': form_entry
            }]
        }
