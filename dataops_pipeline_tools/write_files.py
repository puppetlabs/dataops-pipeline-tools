import jsonlines
import json

from smart_open import open as smartopen

def write_to_file(data: dict, outfile: str, schema: dict) -> None:
    """Write data to a json file in either newline-delimited or pretty json format.
    Write schema to file in json format.

    Args:
        data: The data to write to a file.
        outfile: The path to write the file out to.
        schema: The schema dictionary to write to a schema file
    Returns:
        None
    """
    with jsonlines.open(f"{outfile}.ndjson", 'w') as writer:
        for entry in data:
            writer.write(entry)

    with open(f"{outfile}.bqschema.json", "w") as writer:
        json.dump(schema, writer, indent=2, sort_keys=True)

def stream_data_to_gcs(data: dict, bucket: str, service: str, endpoint: str, date: str) -> None:
    """Stream data in newline-delimited json format to Google Cloud Storage.

    Args:
        data: The data to write to a file
        bucket: The gcs bucket to stream to
        service: The service we are getting data from
        endpoint: The API endpoint we are getting data for
        date: The current date to use in the filename
    Returns:
        None
    """
    with smartopen(
        f"gs://{bucket}/{service}/{endpoint}/{endpoint}-{date}.ndjson", "w"
    ) as file_out:
        for item in data:
            file_out.write(f"{json.dumps(item)}\n")

def stream_schema_to_gcs(schema: dict, bucket: str, service: str, endpoint: str) -> None:
    """
    Stream schema to GCS in json format.

    Args:
        schema: The schema data to write to a file
        bucket: The gcs bucket to stream to
        service: The service we are getting data from
        endpoint: The API endpoint we are getting data for
    Returns:
        None
    """
    with smartopen(
        f"gs://{bucket}/{service}/{endpoint}/{endpoint}.schema.json", "w"
    ) as file_out:
        file_out.write(json.dumps(schema))
