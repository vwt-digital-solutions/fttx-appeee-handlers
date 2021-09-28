import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def _get_request_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=Retry(
        total=6,
        read=6,
        connect=6,
        backoff_factor=10,
        status_forcelist=(404, 500, 502, 503, 504),
    ))
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class GISService:
    _AUTH_URL = "https://geoportaal.vwinfra.nl/portal/sharing/rest/generateToken"
    _ARCGIS_URL = "https://geoportaal.vwinfra.nl/server/rest/services/Hosted/BOPregistratie_testomgeving/FeatureServer"

    _REQUEST_SESSION = _get_request_session()

    def __init__(self, token: str):
        self.token = token

    def query_feature_ids(self, feature_layer: int, query: str) -> list:
        features = self._make_arcgis_request(
            action="query",
            feature_layer=feature_layer,
            data={
                "where": query,
                "outFields": "objectid"
            }
        )["features"]

        return [feature["attributes"]["objectid"] for feature in features]

    def get_feature(self, feature_layer: int, feature_id: int) -> dict:
        return self._make_arcgis_request(
            action="",
            feature_layer=feature_layer,
            feature_id=feature_id
        )["feature"]

    def get_attachments(self, feature_layer: int, feature_id: int) -> list:
        response = self._make_arcgis_request(
            action="attachments",
            feature_layer=feature_layer,
            feature_id=feature_id
        )
        return response["attachmentInfos"]

    def delete_attachments(self, feature_layer: int, feature_id: int, attachment_ids: list) -> dict:
        response = self._make_arcgis_request(
            action="deleteAttachments",
            feature_layer=feature_layer,
            feature_id=feature_id,
            data={
                "attachmentIds": ", ".join(map(str, attachment_ids))
            }
        )
        return response["deleteAttachmentResults"]

    def delete_features(self, feature_layer: int, feature_ids: list):
        for feature_id in feature_ids:
            attachments = self.get_attachments(feature_layer, feature_id)
            if attachments:
                attachment_ids = [int(attachment["id"]) for attachment in attachments]
                self.delete_attachments(feature_layer, feature_id, attachment_ids)

        response = self._make_arcgis_request(
            action="deleteFeatures",
            feature_layer=feature_layer,
            data={
                "objectIds": ", ".join(map(str, feature_ids))
            }
        )
        return response["deleteResults"]

    def _make_arcgis_request(
            self,
            action: str,
            feature_layer: int,
            feature_id: int = None,
            data: dict = None,
            files: list = None
    ):
        request_data = {"f": "json", "token": self.token}
        if data:
            request_data.update(data)

        url = f"{self._ARCGIS_URL}/{feature_layer}"
        if feature_id is not None:
            url = f"{url}/{feature_id}"

        url = f"{url}/{action}"
        response = self._REQUEST_SESSION.post(url, data=request_data, files=files)
        data = response.json()

        if "error" in data:
            print(f"An error occurred: {data['error']}")

        return data

    @classmethod
    def login(cls, username: str, password: str):
        request_data = {
            "f": "json",
            "username": username,
            "password": password,
            "request": "gettoken",
            "referer": "https://geoportaal.vwinfra.nl/portal"
        }

        response = cls._REQUEST_SESSION.post(cls._AUTH_URL, request_data)
        data = response.json()

        return cls(data["token"])
