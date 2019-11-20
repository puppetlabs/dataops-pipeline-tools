import jsonlines
import json

def write_to_file(data: dict, outfile: str, schema: dict) -> None:
    """Write data to a json file in either newline-delimited or pretty json format.

    Args:
        data: The data to write to a file.
        outfile: The path to write the file out to.
    Returns:
        None
    """
    with jsonlines.open(f"{outfile}.ndjson", 'w') as writer:
        for entry in data:
            writer.write(entry)

    with open(f"{outfile}.bqschema.json", "w") as writer:
        json.dump(schema, writer, indent=2, sort_keys=True)