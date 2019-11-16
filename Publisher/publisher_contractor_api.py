from requests import post, get
from Utils.models.schemas import Student
from json import dumps, loads
from Utils.cryptors import Cryptor
from Utils.key_operations import get_key


url = "http://localhost:10000/v1/publisher/{}"


def register_client(client_name):
    args = {"publisher_name": client_name}
    response = post(url.format("register_publisher"), data=args)
    print(response.status_code, response.json())


def unregister_client(client_name):
    args = {"publisher_name": client_name}
    response = post(url.format("unregister_publisher"), data=args)
    print(response.status_code, response.json())


def add_new_table(client_name, table_name, schema):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema}
    response = post(url.format("define_new_table"), data=args)
    print(response.status_code, response.json())


def delete_table(client_name, table_name, schema):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema}
    response = post(url.format("drop_table"), data=args)
    print(response.status_code, response.json())


def add_data(client_name, table_name, schema, data_list):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema, "data": data_list}
    response = post(url.format("add_data"), data=args)
    print(response.status_code, response.json())


def get_data(client_name, table_name, schema):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema}
    response = post(url.format("get_data"), data=args)

    data = []
    if response.json().get("status"):
        data = loads(response.json().get("data"))

    return data


def encrypt_data(data_list):
    fernet_key = get_key()
    crypt = Cryptor(fernet_key)
    encrypted_data = []

    for data in data_list:
        encrypted_data.append(crypt.encrypt(data))
    return encrypted_data


if __name__ == '__main__':

    client_name_ = "UMD"
    table_name_ = "Student"

    # Start from a clean slate
    # unregister_client(client_name_)
    # register_client(client_name_)

    # Create schema in our new format
    schema_ = dumps([x.get_object() for x in Student])
    # add_new_table(client_name_, table_name_, schema=schema_)

    # Create some dummy data
    with open("../Data/dataNov-16-2019.csv") as f:
        data = f.read()

    data_to_add = []
    for d in data.splitlines()[1:]:
        info = d.strip().split(",")
        data_to_add.append({"name": info[0], "age": info[1], "department": info[2], "registered": info[3]})

    # Encrypt the data
    encrypted_data = encrypt_data(data_to_add)
    data = dumps(encrypted_data)

    # Add encrypted data to the newly created table
    add_data(client_name_, table_name_, schema=schema_, data_list=data)
