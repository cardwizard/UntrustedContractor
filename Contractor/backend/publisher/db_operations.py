from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy import create_engine
from Contractor.constants import POSTGRES_CONNECTION

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
