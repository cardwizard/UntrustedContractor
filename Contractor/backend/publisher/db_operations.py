from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from Contractor.constants import POSTGRES_CONNECTION

column_map = {"Integer": Integer, "String": String, "Boolean": Boolean, "JSON": JSON}


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
        if is_primary_key:
            new_attributes[column_name] = Column(column_map[column_type], primary_key=True)
        else:
            new_attributes[column_name] = Column(column_map[column_type])
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
    NewSchema, Base = get_schema(attributes)

    session = get_session(db_name)
    add_list = []

    for data in data_to_add:
        add_list.append(NewSchema(**data))

    session.add_all(add_list)
    session.commit()
    session.close()


def get_data(attributes, db_name):
    NewSchema, Base = get_schema(attributes)
    session = get_session(db_name)
    data = []

    information = session.query(NewSchema).all()
    del attributes["__tablename__"]

    for info_ob in information:
        new_ob = {}

        for attr in attributes:
            new_ob[attr] = getattr(info_ob, attr)
        data.append(new_ob)

    return data

