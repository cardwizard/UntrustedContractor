from requests import post, get
from Utils.models.schemas import SQLObject, Types, SQLSchema
from json import dumps, loads
from Utils.cryptors import Cryptor
from Utils.key_operations import get_key
from Publisher.projection_generator import Projection

url = "http://localhost:10000/v1/publisher/{}"

# Defining a table class
Student = SQLSchema([SQLObject("id", Types.INT),
           SQLObject("name", Types.STR, True),
           SQLObject("age", Types.STR),
           SQLObject("department", Types.STR),
           SQLObject("registered", Types.STR)])


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


def encrypt_data(data_list, skip_keys):
    fernet_key = get_key()
    crypt = Cryptor(fernet_key)
    encrypted_data = []

    for data in data_list:
        encrypted_data.append(crypt.encrypt(data, skip_keys))
    return encrypted_data


def create_projections(data_to_add):
    p_age = Projection("int")
    schema_age, projection_age = p_age.create_projections([{"column": int(x["age"]), "proj_id": x["id"]} for x in data_to_add],
                                              [(10, 15), (15, 18), (18, 20), (20, 23), (23, 26), (26, 29), (30, 100)])

    p_name = Projection("str")
    schema_name, projection_name = p_name.create_projections([{"column": x["name"], "proj_id": x["id"]} for x in data_to_add],
                                                [chr(x) for x in range(ord('A'), ord('Z'))])

    p_dept = Projection("str")
    schema_dept, projection_dept = p_dept.create_projections([{"column": x["department"], "proj_id": x["id"]} for x in data_to_add],
                                                ['ENEE', 'CMSC', 'COMM', 'HUMA', 'ENTC'])

    p_reg = Projection("identity")
    schema_reg, projection_registered = p_reg.create_projections([{"column": x["registered"], "proj_id": x["id"]} for x in data_to_add],
                                                     [])
    return [{"column": "age", "schema": schema_age, "projection": projection_age},
            {"column": "name", "schema": schema_name, "projection": projection_name},
            {"column": "department", "schema": schema_dept, "projection": projection_dept},
            {"column": "registered", "schema": schema_reg, "projection": projection_registered}]


def add_projection(client_name, table_name, column_name, schema, data_list):
    args = {"publisher_name": client_name, "table_name": table_name, "column": column_name, "schema": schema,
            "data": dumps(data_list)}
    response = post(url.format("add_projection"), data=args)
    print(response.status_code, response.json())


if __name__ == '__main__':

    client_name_ = "UMD"
    table_name_ = "Student"

    # Start from a clean slate
    unregister_client(client_name_)
    register_client(client_name_)


    # Create schema in our new format
    schema_ = Student.get_schema()
    add_new_table(client_name_, table_name_, schema=schema_)
    #
    # Create some dummy data
    with open("../Data/dataNov-16-2019.csv") as f:
        data = f.read()

    with open("../Data/dataNov-16-2019-1.csv") as f:
        data2 = f.read()

    data = data.splitlines()[1:]
    data2 = data2.splitlines()[1:]
    data.extend(data2)

    data_to_add = []

    for id, d in enumerate(data):
        info = d.strip().split(",")
        data_to_add.append({"id": id, "name": info[0], "age": info[1], "department": info[2],
                            "registered": info[3]})

    # Encrypt the data
    encrypted_data = encrypt_data(data_to_add, ["id"])
    data = dumps(encrypted_data)

    # Add encrypted data to the newly created table
    add_data(client_name_, table_name_, schema=schema_, data_list=data)
    projections = create_projections(data_to_add)

    for proj in projections:
        add_projection(client_name=client_name_, table_name=table_name_, column_name=proj["column"],
                       schema=proj["schema"], data_list=proj["projection"])
