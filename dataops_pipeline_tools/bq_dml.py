"""
Creates a class BigQueryDML that generates DML queries for Google BigQuery.
Can dynamically generate SELECT, INSERT, and UPDATE DML queries based on
a dictionary input to the class.
"""

import logging
import datetime

from dateutil.parser import parse

from google.cloud import bigquery
from google.cloud import exceptions
from google.cloud.bigquery.table import RowIterator, Row


class BigQueryDML:
    """
    A class that allows generation of DML queries, and converts
    dictionaries to DML queries

    Args:
        bq_project: The Google Cloud project the BigQuery instance you are
                    reading/writing from lives in
        bq_dataset: The BigQuery dataset that you are reading/writing from
        bq_table: The BigQuery table you are reading/writing from
    """

    def __init__(self, bq_project, bq_dataset, bq_table):

        self.project = bq_project
        self.client = bigquery.Client(project=bq_project)
        self.dataset = bq_dataset
        self.table = bq_table
        self.table_ref = f"{bq_project}.{bq_dataset}.{bq_table}"

    @staticmethod
    def create_bq_payload(data: Row):
        """
        Takes in a row object from BigQuery and returns a dictionary
        of key value pairs based on the row columns and values

        Args:
            data: The Row object from the BigQuery API
        Returns:
            A python dictionary object
        """

        results = {k:v for k, v in data.items()}

        return results

    @staticmethod
    def convert_bq_datetime_to_str(data: dict, date_format: str) -> dict:
        """
        Converts a datetime object to a datetime string that matches the
        datetime string we get back from Zendesk, for comparison against live
        Zendesk records

        Args:
            data: The dictionary representation of a BigQuery row of data
            date_format: The format string for how you'd like the datetime represented
        Returns:
            The dictionary with datetimes converted to formatted datetime strings
        """

        for key, value in data.items():
            if isinstance(value, datetime.datetime):
                data[key] = value.strftime(date_format)

        return data

    @staticmethod
    def __check_timestamp(timestamp: str) -> str:
        """
        Checks if the values of a timestamp fields is None.
        If None, convert to NULL string for SQL query, otherwise
        wrap with extra quotes for SQL query

        Args:
            timestamp: The timestamp value to assess
        Returns
            A string, either the timestamp with extra quotes or NULL
        """

        if timestamp:
            result = f'"{timestamp}"'
        else:
            result = "NULL"

        return result

    def alter_timestamps(self, data: dict) -> dict:
        """
        Takes in a list of keys that represent values that are timestamps in data dictionary.
        Passes each key's value to check_timestamp, then updates dictionary key to use
        value returned by check_timestamp

        Args:
            data: The dictionary to update values for
            keys: The list of keys that represent datetime values in the dictionary
        Returns:
            The dictionary with updated datetime values
        """

        for key, value in data.items():
            if isinstance(value, dict):
                self.alter_timestamps(value)
            try:
                parse(data[key])
                timestamp = self.__check_timestamp(data[key])
                data[key] = timestamp
            except:
                logging.debug(f"Value for {key} is {value}, not a timestamp")

        return data

    @staticmethod
    def parse_insert_columns(data: dict, **kwargs) -> str:
        """
        Parses keys of a dictionary into BigQuery DML compliant
        column string for first half of a DML INSERT query

        Args:
            data: The dictionary of data to pull keys that map to columns
            **kwargs: Used to manage the DML Query string for nested dicts in the data dictionary.
                    Can be used to pass any number of key,value args
        Returns:
            The first half of an INSERT DML query string
        """

        key_string = kwargs.get("base_string", "(")

        for key, _ in data.items():
            key_string = key_string + f"{key}, "

        if key_string.endswith(", "):
            key_string = f"{key_string[:-2]}) "

        return key_string

    def parse_insert_values(self, data: dict, **kwargs) -> str:
        """
        Parses values of a dictionary into BigQuery DML compliant
        values string for second half of a DML INSERT query

        Args:
            data: The dictionary of data to pull values that map to column values
            **kwargs: Used to manage the DML Query string for nested dicts in the data dictionary.
                    Can be used to pass any number of key,value args
        Returns:
            The second half of an INSERT DML query string
        """

        values = kwargs.get("base_string", "")

        for _, value in data.items():
            print(value)
            print(type(value))

            if isinstance(value, str):
                if "https://" in value:
                    value = f'"{value}"'
                values = values + f"{value}, "

            elif isinstance(value, datetime.datetime):
                value = self.__check_timestamp(value.strftime("%Y-%m-%dT%H:%M:%SZ"))
                values = values + f"{value}, "

            elif isinstance(value, int):
                values = values + f"{value}, "

            elif isinstance(value, list):
                child = "["
                for item in value:
                    if isinstance(item, dict):
                        child = child + f"({self.parse_insert_values(item).rstrip(', ')}), "
                    else:
                        child = child + f"{item}, "
                if child.endswith(", "):
                    child = child[:-2]
                values = values + f"{child}], "

            elif isinstance(value, dict):
                child = self.parse_insert_values(value)
                if child.endswith(", "):
                    child = f"{child[:-2]}"
                else:
                    child = f"{child}"
                values = values + f"({child}), "

            else:
                values = values + "NULL, "

        return values

    def parse_insert_query_data(self, data: dict) -> str:
        """
        Parses a dictionary into a full BigQuery DML INSERT query

        Args:
            data: The dictionary of data to parse into a DML query
            table_ref: The reference to the table data with be inserted into
        Returns:
            A full DML query to insert a record
        """

        query_first_half = f"INSERT `{self.table_ref}` ("
        query_second_half = "VALUES("

        keys = self.parse_insert_columns(data, base_string=query_first_half)
        values = self.parse_insert_values(data, base_string=query_second_half)

        if values.endswith(", "):
            values = values[:-2]

        results = f"{keys}{values})"
        return results

    def parse_update_query_data(self, data: dict, **kwargs) -> str:
        """
        Parses a dictionary into a BigQuery DML UPDATE query.

        Args:
            data: The dictionary of data to parse into a DML query
            table_ref: The reference to the table data with be inserted into
            **kwargs: Used to pass parent key for nested dictionaries in data
        Returns:
            A BigQuery DML UPDATE query
        """

        query = f"UPDATE {self.table_ref} SET "

        # If we have nested dictionaries we can set a parent key
        # that is added to each sub-dict key in the DML string when
        # the function is called recursively.
        # This can be seen on line 358
        parent_key = kwargs.get("parent_key", None)

        # If we have nested dictionaries we can pass the current
        # query that is partially built back into the function
        # so it can be appended to when called recursively
        current_query = kwargs.get("current_query", None)

        if current_query:
            query = current_query

        for key, value in data.items():
            if parent_key:
                key = f"{parent_key}.{key}"
            if isinstance(value, str):
                if "https://" in value:
                    value = f'"{value}"'
                query = query + f"{key} = {value}, "

            elif isinstance(value, int):
                query = query + f"{key} = {value}, "

            elif isinstance(value, datetime.datetime):
                value = self.__check_timestamp(value.strftime("%Y-%m-%dT%H:%M:%SZ"))
                query = query + f"{key} = TIMESTAMP({value}), "

            elif isinstance(value, list):
                string = []
                for item in value:
                    if isinstance(item, dict):
                        child = self.parse_update_query_data(item, current_query='STRUCT(')
                        child.rstrip(", ")
                        string.append(f"{child}), ")
                    else:
                        child = f"{item}"
                        string.append(child)
                formatted_string = ', '.join(string).rstrip(", ")
                print(formatted_string)
                query = query + f"{key} = [{formatted_string}], "

            elif isinstance(value, dict):
                query = self.parse_update_query_data(
                    value, parent_key=key, current_query=query
                )

            elif not value:
                query = query + f"{key} = NULL, "

        return query

    def generate_sql_query(self, **kwargs) -> str:
        """
        Generates BigQuery DML queries

        Args:
            **kwargs: Used due to variable number of arguments that can be passed.
            Sub-args:
                data: The dictionary you want to create an insert or update DML statement for
                query_type: The type of query, e.g. SELECT, INSERT, or UPDATE
                query_select: A selector for a SELECT statement, if not passed * is used
                query_where: A WHERE clause statement, e.g. WHERE id = data['id']
        Returns:
            A full, BigQuery compliant DML query string
        """

        data = kwargs.get("data", None)
        query_type = kwargs.get("query_type", "SELECT")
        query_select = kwargs.get("query_selector", None)
        query_where = kwargs.get("query_where", None)

        # Build select queries
        if query_type == "SELECT":
            if query_select and not query_where:
                complete_query = f"SELECT {query_select} FROM {self.table_ref}"
            elif not query_select and query_where:
                complete_query = f"SELECT * FROM {self.table_ref} {query_where}"
            elif query_select and query_where:
                complete_query = (
                    f"SELECT {query_select} FROM {self.table_ref} {query_where}"
                )
            else:
                complete_query = f"SELECT * FROM {self.table_ref}"

        # Build insert queries
        elif query_type == "INSERT":

            complete_query = self.parse_insert_query_data(data)

        # Build Update Queries
        elif query_type == "UPDATE":

            query = self.parse_update_query_data(data)

            if query.endswith(", "):
                query = query[:-2]

            if query_where:
                complete_query = f"{query} {query_where}"
            else:
                complete_query = query

        else:
            raise ValueError("query_type not one of SELECT, INSERT, UPDATE")

        return complete_query

    def run_bq_query(self, query: str) -> RowIterator:
        """
        Queries a bq table for all entries

        Args:
            bq_client: the bigquery client object
            query: the sql query we are running against the dataset
        Returns:
            A bq response object
        """
        bq_query = self.client.query(query)
        try:
            results = bq_query.result()
            bq_results = results
        except exceptions.NotFound:
            logging.error("Table not found for query: %s", query)
            bq_results = None

        return bq_results

    def insert_record(self, data: dict) -> str:
        """
        Runs an INSERT DML query against a BigQuery table

        Args:
            bq_client: An authenticated BigQuery Client object
            data: The dictionary payload to load into BigQuery
            table_ref: A table reference to a table in BigQuery, format: project.dataset.table
        Returns:
            The results from the API call
        """

        insert_query = self.generate_sql_query(query_type="INSERT", data=data)

        results = self.run_bq_query(insert_query)

        return results

    def update_record(self, data: dict) -> str:
        """
        Runs an UPDATE DML query against a BigQuery table

        Args:
            bq_client: An authenticated BigQuery Client object
            data: The dictionary payload to load into BigQuery
            table_ref: A table reference to a table in BigQuery, format: project.dataset.table
        Returns:
            The results from the API call
        """

        query_where = f"WHERE id = {data['id']}"

        update_query = self.generate_sql_query(
            query_type="UPDATE", query_where=query_where, data=data
        )

        results = self.run_bq_query(update_query)

        return results

    def insert_or_update(self, data: dict) -> str:
        """
        Checks a BigQuery table to see if a record of the id in the data dict exists.
        If it does, generates and runs an UPDATE query against the record.
        If it does not, generates and runs an INSERT query against the record.

        Args:
            bq_client: An authenticated BigQuery Client object
            data: The dictionary that needs loaded into BigQuery
            table_ref: A table reference to a table in BigQuery, format: project.dataset.table
        Returns:
            A success or failure message from the query run
        """

        query_where = f"WHERE id = {data['id']}"

        search_query = self.generate_sql_query(
            query_type="SELECT", query_where=query_where
        )

        check_for_rows = self.run_bq_query(search_query)

        if check_for_rows.total_rows == 0:

            logging.info(f"INSERTING_RECORD {data['id']}")
            insert = self.insert_record(data)

            if insert:
                results = f"INSERT successful for ticket metric {data['id']}"
            else:
                results = f"INSERT failed for ticket metric {data['id']}"

        else:

            logging.info(f"UPDATING RECORD: {data['id']}")
            update = self.update_record(data)

            if update:
                results = f"UPDATE successful for ticket metric {data['id']}"
            else:
                results = f"UPDATE failed for ticket metric {data['id']}"

        return results
