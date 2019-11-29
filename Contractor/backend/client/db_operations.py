from pathlib import Path
from json import load
from Contractor.backend.publisher.db_operations import get_data, build_schema, get_schema, \
    get_session, convert_query_to_data
from sqlalchemy.sql import func
from sqlalchemy import or_, and_


function_map_func = {"max": func.max, "min": func.min, "count": func.count, "sum": func.sum, "avg": func.avg}
count_functions = ["count"]


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


def check_int_schema(col_schema):

    for info in col_schema:
        if info["Column"] == "end":
            return True
    return False


def filter_for_aggregations(client_name, table_name, aggregation_info, id_list=None, where_passed=False):
    if where_passed and id_list is None:
        return []

    where_id_list = id_list

    column_name = "agg_{}".format(aggregation_info["column_name"])
    col_schema = get_local_schema(client_name, table_name, column_name)
    agg_function = aggregation_info["function"]

    if not check_int_schema(col_schema) and agg_function in count_functions:
        return []

    attributes = build_schema("projection_{}".format(column_name), col_schema)

    session = get_session(client_name)
    ProjectionSchema, Base = get_schema(attributes)

    match_attr = "proj_id" if agg_function in count_functions else "end"

    if where_passed:
        agg_evaluated = session.query(function_map_func[agg_function](getattr(ProjectionSchema, match_attr)))\
            .filter(ProjectionSchema.proj_id.in_(where_id_list)).all()
    else:
        agg_evaluated = session.query(function_map_func[agg_function](getattr(ProjectionSchema, match_attr))).all()

    if agg_function in count_functions:
        query_o = session.query(getattr(ProjectionSchema, match_attr)).all()
        agg_id_list = [x[0] for x in query_o]

        return list(set(agg_id_list) & set(where_id_list)) if where_passed else agg_id_list

    if not agg_evaluated:
        return []

    agg_value = agg_evaluated[0][0]

    where_attr = {"matching_type": "equals", "value": agg_value}

    f = Filter()
    f = f.filter_by_where(client_name, table_name, aggregation_info["column_name"], where_attr=where_attr)

    if where_passed:
        f = f._merge(where_id_list)

    return f.get_id_list()


def filter_by_groups(client_name, table_name, query):

    """
    Group By Queries work on very specific queries as of now. Will only work if you run a group by on a string based
    column and do the aggregations min and max on a column having an integer.

    Currently, only one projection for the view age_dept exists which makes it only work for min-max queries for
    department and age.
    """
    aggregation = query["aggregations"]
    group_by_column = query["by"]

    view_name = "view_" + "_".join(sorted([aggregation["column"], group_by_column]))
    function = aggregation["function"]

    schema = get_local_schema(client_name, table_name, view_name)
    attributes = build_schema("{}".format("view_age_department"), schema)
    session = get_session(client_name)

    ProjectionSchema, Base = get_schema(attributes)
    grouped_info = session.query(ProjectionSchema.startswith,
                                 function_map_func[function](ProjectionSchema.end))\
        .group_by(ProjectionSchema.startswith).all()

    mega_query = []

    for agg_info in grouped_info:
        sub_query = and_(ProjectionSchema.end == agg_info[1], ProjectionSchema.startswith == agg_info[0])
        mega_query.append(sub_query)

    query2 = session.query(ProjectionSchema.proj_id).filter(or_(*mega_query))

    info = query2.all()

    id_list = [x[0] for x in info]
    return id_list
