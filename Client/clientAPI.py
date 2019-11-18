from requests import get, post
from Utils.cryptors import Cryptor
from Utils.models.schemas import SQLObject, Types, SQLSchema
from json import loads, dumps
from Utils.key_operations import get_key

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

    for data in encrypted_data:
        decrypted_data.append(crypt.decrypt(data))
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


def get_data_from_publisher(client_name, table_name, schema):
    encrypted_data = get_data(client_name, table_name, schema=schema)
    return decrypt_data(encrypted_data)


if __name__ == '__main__':
    client_name_ = "UMD"
    table_name_ = "Student"
    schema_ = Student.get_schema()

    print(decrypt_data(get_data_by_id(client_name_, table_name_, schema_, [1, 2, 3])))
    age_projection_schema = SQLSchema([SQLObject("start", Types.INT),
                                       SQLObject("end", Types.INT),
                                       SQLObject("proj_id", Types.INT, True)])

    print(get_data(client_name_, table_name="projection_age", schema=age_projection_schema.get_schema()))
