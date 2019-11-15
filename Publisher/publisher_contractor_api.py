from requests import post
from Publisher.models.schemas import Student
from json import dumps


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
    response = post(url.format("add_new_table"), data=args)
    print(response.status_code, response.json())


if __name__ == '__main__':
    # register_client("UMD")
    add_new_table("UMD", "Student", dumps([x.get_object() for x in Student]))
