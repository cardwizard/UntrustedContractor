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


if __name__ == '__main__':
    client_name_ = "UMD"

    register_client(client_name_)
    schema_ = dumps([x.get_object() for x in Student])

    add_new_table(client_name_, "Student", schema=schema_)

    data = dumps([{"name": "Aadesh", "age": 25, "department": "CMSC", "registered": True}])
    add_data(client_name_, "Student", schema=schema_, data_list=data)
    
    # delete_table(client_name_, "Student", schema=schema_)
    # unregister_client(client_name_)

