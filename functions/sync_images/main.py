import logging

logging.basicConfig(level=logging.INFO)


def handler(request):
    message = "Hello World!"
    logging.info(message)
    return message


if __name__ == "__main__":
    request = None
    handler(request)
