# dataops-pipeline-tools
A python3 library for various functions that the dataops team uses in most data pipeline extract/transform/load containers

## Setup:

### Installation:
This package is not currently hosted in PyPi, please install with pip3 from GitHub.
Ensure you're using python3.6 or higher

`pip3 install git+git://github.com/puppetlabs/dataops-pipeline-tools.git`

### Usage:
Import the package submodules you want to use. Currently there are four:
```python
from dataops_pipeline_tools import bq_data_operations
from dataops_pipeline_tools import bq_schema_generator
from dataops_pipeline_tools import write_files
from dataops_pipeline_tools import request_sessions
```

#### bq_data_operations
These functions modify data structures to be Google BigQuery compliant. Example usage for if you have a dictionary
of values you want to make sure is compliant and able to be written to an ndjson file for BQ upload:

```python
import json
from dataops_pipeline_tools import bq_data_operations.to_gbq_keys
from dataops_pipeline_tools import bq_data_operations.drop_empty_iters

obj = [
    {
        'key': 'value',
        'another_key': 'another_value'
    },
    {
        'key': 'value',
        'another_key': 'another_value'
    },
    {
        'key': [],
        'another_key': 'value'
    }
]

results = [
    json.loads(
        json.dumps(item)
        object_hook=to_gbq_keys)
    )
    for item in obj
]

parsed_results = drop_empty_iters(results)
```
The above example will go through the obj dictionary and replace all keys with bq compliant keys, setting the result to `results`.
It then will go through the results and remove key/value pairs where the value is an empty list, dict, or a None value. That
result is set to `parsed_results`. 

#### bq_schema_generator
This has one function, which uses the bq-schema-generator library's `SchemaGenerator()` class to generate a schema
for data that is being uploaded to BQ. Documentation for that library can be fond here: 
https://github.com/bxparks/bigquery-schema-generator
An example of usage:
```python
from dataops_pipeline_tools import bq_schema_generator.generate_bq_schema

obj = [
    {
        'key': 'value',
        'another_key': 'another_value'
    },
    {
        'key': 'value',
        'another_key': 'another_value'
    }
]

# You can pass an optional schema options dictionary to the function for arguments to be passed to `SchemaGenerator()`

options = {
    'input_format': None,
    'infer_mode': False,
    'keep_nulls': False,
    'quoted_values_are_strings': True,
    'debugging_interval': 500,
    'debugging_map': False
}

schema = generate_bq_schema(obj, options=options)
```

#### request_sessions
This has one function, which takes a requests session object and url, and an optional params argument for parameters to pass
in the request. It returns a response object in json format. 
An example of usage:
```python
from requests import Session
from dataops_pipeline_tools.requests_sessions import perform_get

session = Session()
url = 'https://exmple.com/api/

response = perform_get(session, url)
```
#### write_files
This has one function, which takes a data dictionary, an output file string, and a schmea dictionary as arguments. It then
writes the data to an ndjson file, and the schema to a json file with the name specified from the outfile. 
An example of usage:
```python
from dataops_pipeline_tools import bq_schema_generator.generate_bq_schema
from dataops_pipeline_tools.write_files import write_to_file

obj = [
    {
        'key': 'value',
        'another_key': 'another_value'
    },
    {
        'key': 'value',
        'another_key': 'another_value'
    }
]

schema = generate_bq_schema(obj)

write_to_file(obj, 'example', schema)
```

You will end up with two files, named `example.ndjson` and `example.bqschema.json`. 
