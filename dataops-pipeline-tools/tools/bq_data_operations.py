import re

def str_to_gbq_field_name(in_str: str) -> str:
    """Transform a string into a valid BigQuery field name, based on Google's
    description of BigQuery field name requirements:

        The name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_),
        and must start with a letter or underscore. The maximum length is 128 characters.

    The following steps are performed on the input string to ensure a BQ field-compliant string:
        * Strip leading/trailing whitespace
        * Substitute non-alphanumeric characters with underscores
        * If first char is a digit, prefix with an underscore
        * Return only the first 128 characters of the string

    Args:
        in_str: Input string to transform to a BigQuery-compliant string.

    Returns:
        A transformed string according to the rules stated above.

    """
    if not isinstance(in_str, str):
        raise TypeError(f"Invalid input string: {in_str}")

    tmp_str = "".join([char if char.isalnum() else "_" for char in in_str.strip()])

    # Prefix field name with _ if first character is a digit
    if re.match(r"\d", tmp_str[0]):
        gbq_str = "_" + tmp_str
    else:
        gbq_str = tmp_str

    return gbq_str[:128]

def to_gbq_keys(obj: dict) -> dict:
    """Returns an identical dict to the input dict, except top-level keys are
    replaced with Google BigQuery-field-compliant keys.
    This function should be used with the object_hook arg to json.dumps().
    When used as an object_hook, all dictionary keys will be recursively
    changed to BQ-compliant keys.

    Example:
        gbq_compliant_data = json.dumps(data, object_hook=to_gbq_keys)

    Args:
        obj: A dict.

    Returns:
        A dict with BigQuery-compliant keys.

    """
    new_obj = {}
    for key, value in obj.items():
        new_key = str_to_gbq_field_name(key)
        new_obj[new_key] = value

    return new_obj

def drop_empty_iters(obj: dict) -> dict:
    """Drops keys from the dictonary whose values are empty lists or empty dictionaries.
    Args:
        obj: The data structure to rid of empty [] and {} None types.
    Returns:
        An equivalent data structure, minus keys with empty [] and {} values.
    """
    if not isinstance(obj, (dict, list)):
        return obj
    if isinstance(obj, list):
        return [v for v in (drop_empty_iters(member) for member in obj) if v]

    return {k1: v1 for k1, v1
            in ((k2, drop_empty_iters(v2)) for k2, v2 in obj.items())
            if v1 not in ({}, [], None)}