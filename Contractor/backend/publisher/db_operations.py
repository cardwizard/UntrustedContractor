from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from Contractor.constants import POSTGRES_CONNECTION

column_map = {"Integer": Integer, "String": String, "Boolean": Boolean, "JSON": JSON}


def create_db(db_name) -> bool:
    """
    Function to create a database in the background if it does not exist.

    :param db_name:
    :return:
    """
    engine = create_engine(POSTGRES_CONNECTION.format(db_name))
    if database_exists(engine.url):
        return False

    create_database(engine.url)
    return True


def drop_db(db_name) -> bool:
    """

    :param db_name:
    :return:
    """
    engine = create_engine(POSTGRES_CONNECTION.format(db_name))
    if not database_exists(engine.url):
        return False

    drop_database(engine.url)
    return True


def build_schema(table_name, attributes):

    new_attributes = {"__tablename__": table_name}

    for attribute in attributes:
        column_name = attribute["Column"]
        is_primary_key = attribute["Primary Key"]
        column_type = attribute["Type"]
        if is_primary_key:
            new_attributes[column_name] = Column(column_map[column_type], primary_key=True)
        else:
            new_attributes[column_name] = Column(column_map[column_type])
    return new_attributes


def create_table(attributes, db_name):

    Base = declarative_base()
    NewSchema = type("NewSchema", (Base,), attributes)
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
    Base = declarative_base()
    NewSchema = type("NewSchema", (Base,), attributes)

    engine = create_engine(POSTGRES_CONNECTION.format(db_name), echo=True)

    if not engine.dialect.has_table(engine, attributes["__tablename__"]):
        return -1, "Table does not exist"

    try:
        Base.metadata.drop_all(bind=engine, tables=[NewSchema.__table__])
        return 0, "Table dropped successfully"

    except Exception as e:
        print(e.__str__())

    return 1, "Table deletion failed"
