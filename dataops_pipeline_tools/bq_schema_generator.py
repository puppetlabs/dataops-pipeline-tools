import json
import logging
from bigquery_schema_generator.generate_schema import SchemaGenerator

def generate_bq_schema(obj: dict, options: dict = None):
    """Generates a bigquery compliant schema using the bigquery-schema-generator library.

    Args:
        obj: the data structure to create a big query schema for
        options (optional): a dictionary of options to pass to the SchemaGenerator class.
        If nothing is specified default values are used
    Returns:
        A bigquery schema dictionary
    """

    if not options:
        options = {
            'input_format': None,
            'infer_mode': False,
            'keep_nulls': False,
            'quoted_values_are_strings': False,
            'debugging_interval': 500,
            'debugging_map': False
        }

    generator = SchemaGenerator(
        input_format=options['input_format'],
        infer_mode=options['infer_mode'],
        keep_nulls=options['keep_nulls'],
        quoted_values_are_strings=options['quoted_values_are_strings'],
        debugging_interval=options['debugging_interval'],
        debugging_map=options['debugging_map']
        )

    schema_map, error_logs = generator.deduce_schema(
        [json.dumps(item) for item in obj]
    )
    schema = generator.flatten_schema(schema_map)

    if error_logs:
        logging.debug(error_logs)

    return schema