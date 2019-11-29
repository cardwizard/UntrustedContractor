from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy import *
from sqlalchemy.orm import load_only
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from Contractor.constants import POSTGRES_CONNECTION
from pathlib import Path
from typing import List
from json import dump, load


column_map = {"Integer": Integer, "String": String, "Boolean": Boolean, "JSON": JSON, "BINARY": LargeBinary}


def create_db(db_name):
    """
    Function to create a database in the background if it does not exist.

    :param db_name:
    :return:
    """
    engine = create_engine(POSTGRES_CONNECTION.format(db_name))
    if database_exists(engine.url):
        return False, "Database exists already"

    create_database(engine.url)
    return True, "Database created successfully"


def drop_db(db_name):
    """

    :param db_name:
    :return:
    """
    engine = create_engine(POSTGRES_CONNECTION.format(db_name))
    if not database_exists(engine.url):
        return False, "Database does not exist"

    drop_database(engine.url)
    return True, "Database dropped successfully"


def build_schema(table_name, attributes):

    new_attributes = {"__tablename__": table_name}

    for attribute in attributes:
        column_name = attribute["Column"]
        is_primary_key = attribute["Primary Key"]
        column_type = attribute["Type"]
        auto_increment = attribute.get("Auto Increment")

        new_attributes[column_name] = Column(column_map[column_type], primary_key=is_primary_key,
                                             autoincrement=auto_increment)

    return new_attributes


def get_schema(attributes):
    Base = declarative_base()
    NewSchema = type("NewSchema", (Base,), attributes)
    return NewSchema, Base


def create_table(attributes, db_name):

    NewSchema, Base = get_schema(attributes)
    engine = create_engine(POSTGRES_CONNECTION.format(db_name), echo=True)

    if engine.dialect.has_table(engine, attributes["__tablename__"]):
        return -1, "Table exists"

    try:
        Base.metadata.create_all(bind=engine, tables=[NewSchema.__table__])
        return 0, "Table created successfully"

    except Exception as e:
        print(e.__str__())

    return 1, "Table creation failed"


def drop_table(attributes, db_name):

    NewSchema, Base = get_schema(attributes)
    engine = create_engine(POSTGRES_CONNECTION.format(db_name), echo=False)

    if not engine.dialect.has_table(engine, attributes["__tablename__"]):
        return -1, "Table does not exist"

    try:
        Base.metadata.drop_all(bind=engine, tables=[NewSchema.__table__])
        return 0, "Table dropped successfully"

    except Exception as e:
        print(e.__str__())

    return 1, "Table deletion failed"


def get_session(db_name):
    engine = create_engine(POSTGRES_CONNECTION.format(db_name), echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def push_data(attributes, db_name, data_to_add):
    Base = declarative_base()
    NewSchema = type("NewSchema", (Base,), attributes)

    session = get_session(db_name)
    add_list = []

    for data in data_to_add:
        add_list.append(NewSchema(**data))

    session.add_all(add_list)
    session.commit()
    session.close()


def convert_query_to_data(query, attributes, column_list=None):
    if query is None:
        return []

    data = []
    del attributes["__tablename__"]

    for info_ob in query:
        new_ob = {}

        for attr in attributes:
            if column_list and attr not in column_list:
                continue
            new_ob[attr] = getattr(info_ob, attr)

        data.append(new_ob)
    return data


def get_data(attributes, db_name, column_list=None):
    NewSchema, Base = get_schema(attributes)
    session = get_session(db_name)

    if not column_list:
        information = session.query(NewSchema).all()
        return convert_query_to_data(information, attributes)
    column_list += ["id"]

    information = session.query(NewSchema).options(load_only(*column_list)).all()
    return convert_query_to_data(information, attributes, column_list)


def get_data_by_ids(attributes, db_name, id_list, column_list=None):
    NewSchema, Base = get_schema(attributes)
    session = get_session(db_name)

    query = session.query(NewSchema)

    if column_list:
        query = query.options(load_only(*column_list))
        column_list += ["id"]

    query = query.filter(NewSchema.id.in_(id_list))
    information = query.all()

    return convert_query_to_data(information, attributes, column_list)


def cache_schema_locally(client_name: str, table_name: str, projection_name: str, schema: List, proj_prefix: str):
    path = Path("Projections").joinpath(client_name).joinpath(table_name)

    if not path.exists():
        path.mkdir(parents=True)

    with open(path.joinpath(proj_prefix.format(projection_name) + ".json"), "w") as f:
        dump(schema, f)


def get_local_schema(client_name: str, table_name: str, projection_name):
    path = Path("Projections").joinpath(client_name).joinpath(table_name).joinpath("projection_{}.json".format(projection_name))

    if not path.exists():
        return None

    with open(path, "r") as f:
        data = load(f)
        
    return data
