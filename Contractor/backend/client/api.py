from flask import Blueprint, jsonify
from flask_restful import reqparse
from json import loads, dumps
from Contractor.backend.publisher.db_operations import get_data, build_schema, get_data_by_ids
from Contractor.backend.client.db_operations import get_id_list_from_projection, filter_by_where, \
    filter_for_aggregations


client_api = Blueprint("client_api", __name__, url_prefix='/v1/client')


@client_api.route("/status")
def status():
    return jsonify(status=True, info="client")


@client_api.route("/get_all_data", methods=["POST"])
def get_data_():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)

    args = parser.parse_args()
    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    info = get_data(attributes, args["publisher_name"])

    return jsonify(status=True, data=dumps(info))


@client_api.route("/get_data_by_id", methods=["POST"])
def get_data_by_id_():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)
    parser.add_argument("id_list", type=loads)

    args = parser.parse_args()
    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    info = get_data_by_ids(attributes, args["publisher_name"], args["id_list"])

    return jsonify(status=True, data=dumps(info))


@client_api.route("/get_data_by_projections", methods=["POST"])
def get_data_by_projections():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)
    parser.add_argument("column_list", type=loads)

    args = parser.parse_args()

    id_list = get_id_list_from_projection(args["publisher_name"], args["table_name"], args["column_list"][0])

    for column_name in args["column_list"][1:]:
        id_list_new = get_id_list_from_projection(args["publisher_name"], args["table_name"], column_name)
        id_list = list(set(id_list) & set(id_list_new))

    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    info = get_data_by_ids(attributes, args["publisher_name"], id_list, column_list=args["column_list"])

    return jsonify(status=True, data=dumps(info))


@client_api.route("/where", methods=["POST"])
def where_clause():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)
    parser.add_argument("query", type=loads)
    parser.add_argument("column_list", type=loads, default=None)

    args = parser.parse_args()
    attributes = build_schema(args["table_name"], args["alchemy_schema"])

    id_list = []
    where_columns = []

    if "where" in args["query"]:
        id_list = filter_by_where(args["publisher_name"], args["table_name"], args["query"]["where"]["match_criteria"],
                                  link_operation=args["query"]["where"].get("link_operation", "and"))

        for param in args["query"]["where"]["match_criteria"]:
            where_columns.append(param["column_name"])

    if "aggregation" in args["query"]:
        where_passed = True if "where" in args["query"] else False

        id_list = filter_for_aggregations(args["publisher_name"], args["table_name"], args["query"]["aggregation"],
                                          id_list, where_passed)

        if not where_passed and args["query"]["aggregation"]["function"] == "count":
            return jsonify(status=True, data=dumps([len(id_list)]))

        where_columns.extend([args["query"]["aggregation"]["column_name"]])
        args["column_list"] = where_columns

    info = get_data_by_ids(attributes, args["publisher_name"], id_list, column_list=args["column_list"])
    return jsonify(status=True, data=dumps(info))


@client_api.route("/aggregations", methods=["POST"])
def aggregation_clause():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)
    parser.add_argument("query", type=loads)

    args = parser.parse_args()
    attributes = build_schema(args["table_name"], args["alchemy_schema"])

    # id_list = get_data_by_ids(attributes, args["publisher_name"], [x for x in range(1, 20)])
    id_list = [x for x in range(1, 20)]

    return jsonify(status=True)
