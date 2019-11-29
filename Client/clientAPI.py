from requests import get, post
from Utils.cryptors import Cryptor
from Utils.models.schemas import SQLObject, Types, SQLSchema
from json import loads, dumps
from Utils.key_operations import get_key
from Client.client_util import filter_at_client
import pandas as pd

url = "http://localhost:10000/v1/client/{}"

# Defining a table class
Student = SQLSchema([SQLObject("id", Types.INT),
           SQLObject("name", Types.STR, True),
           SQLObject("age", Types.STR),
           SQLObject("department", Types.STR),
           SQLObject("registered", Types.STR)])


def decrypt_data(encrypted_data):

    fernet_key = get_key()
    crypt = Cryptor(fernet_key)
    decrypted_data = []

    try:
        for data in encrypted_data:
            decrypted_data.append(crypt.decrypt(data))
    except:
        return encrypted_data

    return decrypted_data


def get_data(client_name, table_name, schema):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema}
    response = post(url.format("get_all_data"), data=args)

    data = []
    if response.json().get("status"):
        data = loads(response.json().get("data"))

    return data


def get_data_by_id(client_name, table_name, schema, id_list):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema, "id_list": dumps(id_list)}
    response = post(url.format("get_data_by_id"), data=args)

    data = []
    if response.json().get("status"):
        data = loads(response.json().get("data"))

    return data


def get_data_by_projections(client_name, table_name, column_list):
    args = {"publisher_name": client_name, "table_name": table_name, "column_list": dumps(column_list),
            "alchemy_schema": Student.get_schema()}
    response = post(url.format("get_data_by_projections"), data=args)
    return decrypt_data(loads(response.json().get("data")))


def get_data_from_publisher(client_name, table_name, schema):
    encrypted_data = get_data(client_name, table_name, schema=schema)
    return decrypt_data(encrypted_data)


def test_where_clause(client_name, table_name, schema, query, column_list=None):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema, "query":
        query, "column_list": dumps(column_list)}

    response = post(url.format("where"), data=args)

    data = []
    if response.json().get("status"):
        data = loads(response.json().get("data"))

    return data


def test_aggregations(client_name, table_name, schema, query):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema, "query": query}

    response = post(url.format("where"), data=args)
    data = []
    if response.json().get("status"):
        data = loads(response.json().get("data"))

    return data


def test_group_by(client_name, table_name, schema, query):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema, "query": query}

    response = post(url.format("group_by"), data=args)
    data = []
    if response.json().get("status"):
        data = loads(response.json().get("data"))

    return data


if __name__ == '__main__':
    client_name_ = "UMD"
    table_name_ = "Student"
    schema_ = Student.get_schema()


    value_agg = decrypt_data(test_aggregations(client_name_, table_name_, schema_, dumps(query)))
    print(pd.DataFrame(value_agg))
    print()

    query = {
        "group_by":
            {
                "aggregations": {
                    "function": "min",
                    "column": "age"
                },
                "by": "department"
            }
    }

    grouped_values = decrypt_data(test_group_by(client_name_, table_name_, schema_, dumps(query)))

    if query["group_by"]["aggregations"]["function"] == "max":
        print(pd.DataFrame(grouped_values).groupby(["department"]).age.max())

    if query["group_by"]["aggregations"]["function"] == "min":
        print(pd.DataFrame(grouped_values).groupby(["department"]).age.min())

    aggquery = {
        "where": {
            "match_criteria": [
                    {"column_name": "name",
                     "attributes": {"matching_type": "starts_with", "value": 'A'}
                     },
                    # {
                    #     "column_name": "department",
                    #     "attributes": {"matching_type": "starts_with", "value": 'ENEE'}
                    # },
                    {
                        "column_name": "age",
                        "attributes": {"matching_type": "lesser_than", "value": 50}
                    }
                ],

            "link_operation": "and"
        },

        "aggregation": {
            "column_name": "age",
            "function": "count"
            }
        }

    result = decrypt_data(test_aggregations(client_name_, table_name_, schema_, dumps(aggquery)))
    filter_at_client(aggquery, result)
