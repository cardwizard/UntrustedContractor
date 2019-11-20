from pathlib import Path
from json import load
from Contractor.backend.publisher.db_operations import get_data, build_schema, get_data_by_ids, get_schema, get_session, \
    convert_query_to_data


def get_local_schema(client_name: str, table_name: str, projection_name):
    path = Path("Projections").joinpath(client_name).joinpath(table_name).joinpath(
        "projection_{}.json".format(projection_name))

    if not path.exists():
        return None

    with open(path, "r") as f:
        data = load(f)

    return data


def get_id_list_from_projection(client_name: str, table_name, column_name):
    col_schema = get_local_schema(client_name, table_name, column_name)

    attributes = build_schema("projection_{}".format(column_name), col_schema)
    projection_data = get_data(attributes, client_name)

    id_list = [x["proj_id"] for x in projection_data]
    return id_list


def filter_by_where(client_name, table_name, where_query):

    for objects in where_query:

        column_name = objects["column_name"]
        where_attr = objects["attributes"]

        col_schema = get_local_schema(client_name, table_name, column_name)
        attributes = build_schema("projection_{}".format(column_name), col_schema)

        ProjectionSchema, Base = get_schema(attributes)
        session = get_session(client_name)

        if where_attr["matching_type"] == "equals":
            value = where_attr["value"]
            information = session.query(ProjectionSchema).filter(getattr(ProjectionSchema, "start") < value).filter(getattr(ProjectionSchema, "end") > value)

            converted_data = convert_query_to_data(information, attributes)
            return [x["proj_id"] for x in converted_data]
