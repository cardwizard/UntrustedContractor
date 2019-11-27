from pathlib import Path
from json import load
from Contractor.backend.publisher.db_operations import get_data, build_schema, get_data_by_ids, get_schema, get_session, \
    convert_query_to_data
from sqlalchemy.sql import func


function_map_max_min = {"max": func.max, "min": func.min, "count": func.min}


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

    id_list = []

    for x in projection_data:
        id_list.extend(x["proj_id_list"])

    return id_list


class Filter:
    def __init__(self, id_list=None):
        self.id_list = id_list

    @staticmethod
    def _guess_data_type(local_schema):
        ## Hacky code.
        if len(local_schema) == 0:
            return "UNKNOWN"

        return local_schema[1]["Type"]

    @staticmethod
    def _converge_list(converted_data):
        id_list = []

        for x in converted_data:
            id_list.extend(x["proj_id_list"])

        return id_list

    def _filter_by_int(self, client_name, attributes, where_attr):
        session = get_session(client_name)
        ProjectionSchema, Base = get_schema(attributes)

        if where_attr["matching_type"] == "equals":
            value = where_attr["value"]
            information = session.query(ProjectionSchema)\
                .filter(getattr(ProjectionSchema, "start") <= value)\
                .filter(getattr(ProjectionSchema, "end") >= value)

        elif where_attr["matching_type"] == "greater_than":
            value = where_attr["value"]
            information = session.query(ProjectionSchema) \
                .filter(getattr(ProjectionSchema, "end") > value)

        elif where_attr["matching_type"] == "lesser_than":
            value = where_attr["value"]
            information = session.query(ProjectionSchema) \
                .filter(getattr(ProjectionSchema, "start") < value)

        else:
            information = None

        converted_data = convert_query_to_data(information, attributes)

        id_list_ = self._converge_list(converted_data)
        return self._merge(id_list_)

    def _filter_by_str(self, client_name, attributes, where_attr):
        session = get_session(client_name)
        ProjectionSchema, Base = get_schema(attributes)

        if where_attr["matching_type"] == "starts_with":
            value = where_attr["value"]
            information = session.query(ProjectionSchema) \
                .filter(getattr(ProjectionSchema, "startswith").contains(value))
        else:
            information = None

        converted_data = convert_query_to_data(information, attributes)

        id_list_ = self._converge_list(converted_data)

        return self._merge(id_list_)

    def _merge(self, id_list):
        if self.id_list is None:
            self.id_list = id_list
        else:
            self.id_list = list(set(self.id_list) & set(id_list))

        return Filter(self.id_list)

    def filter_by_where(self, client_name, table_name, column_name, where_attr):

        col_schema = get_local_schema(client_name, table_name, column_name)
        attributes = build_schema("projection_{}".format(column_name), col_schema)

        if self._guess_data_type(col_schema) == "Integer" or self._guess_data_type(col_schema) == "Float":
            return self._filter_by_int(client_name, attributes, where_attr)

        elif self._guess_data_type(col_schema) == "String":
            return self._filter_by_str(client_name, attributes, where_attr)

    def get_id_list(self):
        return self.id_list


def filter_by_where(client_name, table_name, where_query, link_operation="and"):

    if link_operation == "and":
        f = Filter()
        for objects in where_query:
            f = f.filter_by_where(client_name, table_name, objects["column_name"], objects["attributes"])
        return f.get_id_list()

    elif link_operation == "or":
        id_list = []

        for objects in where_query:
            f = Filter()
            filtered_query = f.filter_by_where(client_name, table_name, objects["column_name"], objects["attributes"])
            id_list.extend(filtered_query.get_id_list())

        return list(set(id_list))


def filter_for_aggregations(client_name, table_name, aggregation_info, id_list):
    where_id_list = id_list

    column_name = "agg_{}".format(aggregation_info["column_name"])
    col_schema = get_local_schema(client_name, table_name, column_name)
    attributes = build_schema("projection_{}".format(column_name), col_schema)
    agg_function = aggregation_info["function"]

    session = get_session(client_name)
    ProjectionSchema, Base = get_schema(attributes)

    agg_evaluated = session.query(function_map_max_min[agg_function](ProjectionSchema.end))\
        .filter(ProjectionSchema.proj_id
                .in_(id_list)).all()

    if agg_function == "count":
        return agg_evaluated[0][0]

    if not agg_evaluated:
        return []

    agg_value = agg_evaluated[0][0]

    where_attr = {"matching_type": "equals", "value": agg_value}

    f = Filter()
    f = f.filter_by_where(client_name, table_name, aggregation_info["column_name"], where_attr=where_attr)
    f = f._merge(where_id_list)

    return f.get_id_list()
