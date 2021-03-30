import logging
from mimetypes import guess_type
from os import path

import config
import constant
import requests
from google.cloud import storage
from requests_retry_session import get_requests_session


class ImageService:
    def __init__(self, storage_client):
        self.storage_client = storage_client
        self.requests_session = get_requests_session(
            retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
        )

    def _compose_image_store_base_path(self, form_entry):
        if constant.PROVIDER_ID_KEY in form_entry and constant.ENTRY_KEY in form_entry:
            provider_id = form_entry[constant.PROVIDER_ID_KEY]
            ds_row_id = form_entry[constant.ENTRY_KEY].get(constant.DS_ROW_ID_KEY)
            form_code = form_entry[constant.ENTRY_KEY].get(constant.FORM_CODE_KEY)
            if form_code:
                image_store_base_path = (
                    f"{config.IMAGE_STORE_PATH}/{provider_id}/{form_code}/{ds_row_id}"
                )
                logging.info(f"Image store base path is {image_store_base_path}")
                return image_store_base_path

        raise Exception(f"Error composing base_path, {constant.FORM_CODE_KEY} missing")

    def _compose_image_download_base_url(self, form_entry):
        if constant.PROVIDER_ID_KEY in form_entry and constant.ENTRY_KEY in form_entry:
            provider_id = form_entry[constant.PROVIDER_ID_KEY]
            ds_row_id = form_entry[constant.ENTRY_KEY].get(constant.DS_ROW_ID_KEY)
            if ds_row_id:
                image_download_base_url = (
                    f"{config.IMAGE_DOWNLOAD_BASE_URL}/{provider_id}-{ds_row_id}"
                )
                logging.info(f"Image download base url is {image_download_base_url}")
                return image_download_base_url

        raise Exception(
            f"Error composing base_url, {constant.PROVIDER_ID_KEY} or {constant.DS_ROW_ID_KEY} missing"
        )

    def _is_image_to_download(self, answer_key, answer_value):
        if isinstance(answer_value, str):
            _, file_extension = path.splitext(answer_value)
            return file_extension in constant.IMAGE_FILE_EXTENSIONS

        return False

    def download_images_for_form_entry(self, form_entry):
        image_store_bucket = self.storage_client.get_bucket(config.IMAGE_STORE_BUCKET)

        image_download_base_url = self._compose_image_download_base_url(form_entry)
        image_store_base_path = self._compose_image_store_base_path(form_entry)

        pages = form_entry[constant.ENTRY_KEY][constant.ANSWERS_PAGES_KEY]
        for page_k, page_v in pages.items():
            for answer_k, answer_v in page_v.items():
                if self._is_image_to_download(answer_k, answer_v):
                    logging.info(
                        f"Answer on {answer_k} is an image: {answer_v}, start downloading"
                    )
                    file_download_url = f"{image_download_base_url}{answer_v}"
                    file_response = self.requests_session.get(file_download_url)

                    if file_response.status_code == requests.codes.ok:
                        file_store_path = (
                            f"{image_store_base_path}/{answer_k}_{answer_v}"
                        )
                        content_type, _ = guess_type(file_store_path)
                        if storage.Blob(
                            bucket=image_store_bucket, name=file_store_path
                        ).exists(self.storage_client):
                            logging.info(
                                f"Skipping file {file_store_path}. It already exists."
                            )
                        else:
                            logging.info(f"Storing {file_store_path} as {content_type}")
                            image_store_blob = image_store_bucket.blob(file_store_path)
                            image_store_blob.upload_from_string(
                                file_response.content, content_type=content_type
                            )

                        form_entry[constant.ENTRY_KEY][constant.ANSWERS_PAGES_KEY][
                            page_k
                        ][
                            answer_k
                        ] = f"https://storage.cloud.google.com/{config.IMAGE_STORE_BUCKET}/{file_store_path}"
                    else:
                        logging.error(
                            f"Error downloading {file_download_url}, response {file_response.status_code}: {file_response.text}"
                        )

        return form_entry
