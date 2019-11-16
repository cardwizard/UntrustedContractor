from requests import get, post
from Utils.cryptors import Cryptor
from Utils.models.schemas import Student
from json import loads, dumps
from Utils.key_operations import get_key

url = "http://localhost:10000/v1/publisher/{}"


def decrypt_data(encrypted_data):
    fernet_key = get_key()
    crypt = Cryptor(fernet_key)
    decrypted_data = []

    for data in encrypted_data:
        decrypted_data.append(crypt.decrypt(data))
    return decrypted_data


def get_data(client_name, table_name, schema):
    args = {"publisher_name": client_name, "table_name": table_name, "alchemy_schema": schema}
    response = post(url.format("get_data"), data=args)

    data = []
    if response.json().get("status"):
        data = loads(response.json().get("data"))

    return data


def get_data_from_publisher(client_name_, table_name, schema_):
    encrypted_data = get_data(client_name_, table_name, schema=schema_)
    print(decrypt_data(encrypted_data))


if __name__ == '__main__':
    client_name_ = "UMD"
    table_name_ = "Student"
    schema_ = dumps([x.get_object() for x in Student])
    get_data_from_publisher(client_name_, table_name_, schema_)


