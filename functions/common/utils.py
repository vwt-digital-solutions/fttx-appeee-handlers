from google.cloud import secretmanager


def get_from_path(dictionary: dict, path: str):
    """
    Utility function to get a variable from a path.

    Example dictionary:
    {
        "dict": {
            "foo": "bar",
            "list": [
                "zero",
                "one",
                "two"
            ]
        }
    }

    path `dict/foo` will return `bar`
    path `dict/list/1` will return `one`

    :param dictionary: The dictionary to get the variable from.
    :type dictionary: dict
    :param path: The path of the variable in the dictionary.
    :type path: str:

    :return: Returns a variable based on the specified dictionary and path.
    """

    key_list = path.split("/")

    current = dictionary
    for key in key_list:
        if not current:
            break
        elif isinstance(current, dict) and key in current:
            current = current.get(key)
        elif isinstance(current, list) and key.isdigit():
            index = int(key)
            if index >= 0 < len(current):
                current = current[index]
            else:
                current = None
        else:
            current = None

    return current


def get_secret(project_id, secret_id):
    """
    Returns a Secret Manager secret.
    """

    client = secretmanager.SecretManagerServiceClient()

    secret_name = client.access_secret_version(
        request={"name": f"projects/{project_id}/secrets/{secret_id}/versions/latest"}
    )

    payload = secret_name.payload.data.decode("UTF-8")

    return payload
