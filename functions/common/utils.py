import re
from google.cloud import secretmanager


def unpack_ranges(pattern) -> list:
    """
    Unpacks all possible range combinations.

    Range syntax: [{start}-{end}]
        start: start of the range (included)
        end: end of the range (excluded)
    Range example: [1-10]

    While unpacking the ranges will be formatted to stringified numbers.
    There numbers will be justified based on min(len({start}), len({end}))).

    Example 1: '[8-11]'
    Result 1: ['8', '9', '10']

    Example 2: '[08-11]'
    Result 2: ['08', '09', '10']

    Example 3: 'A:[1-3] B:[1-3]'
    Result 3: ['A:1 B:1', 'A:1 B:2', 'A:2 B:1', 'A:2 B:2']

    :param pattern: The pattern to unpack.
    :type pattern: str:

    :return: A list of all possible range combinations.
    :rtype: list[str]
    """
    range_regex = r"\[(\d+)-(\d+)]"
    match = re.search(range_regex, pattern)

    suffixes = []
    if match:
        start = match.group(1)
        end = match.group(2)
        justified = min(len(start), len(end))

        for i in range(int(start), int(end)):
            prefix = pattern[:match.start(0)]
            suffix = pattern[match.end(0):]
            number = str(i).rjust(justified, "0")
            string = f"{prefix}{number}{suffix}"
            suffixes.extend(unpack_ranges(string))
    else:
        suffixes.append(pattern)

    return suffixes


def get_from_path(dictionary: dict, var_path: str):
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
    :param var_path: The path of the variable in the dictionary.
    :type var_path: str:

    :return: Returns a variable based on the specified dictionary and path.
    """

    key_list = var_path.split("/")

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
