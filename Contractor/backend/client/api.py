from flask import Blueprint, jsonify
from flask_restful import reqparse
from json import loads, dumps
from Contractor.backend.publisher.db_operations import get_data, build_schema, get_data_by_ids
from Contractor.backend.client.db_operations import get_local_schema


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
    parser.add_argument("column_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)
    args = parser.parse_args()

    col_schema = get_local_schema(args["publisher_name"], args["table_name"], args["column_name"])

    attributes = build_schema("projection_{}".format(args["column_name"]), col_schema)
    projection_data = get_data(attributes, args["publisher_name"])

    id_list = [x["proj_id"] for x in projection_data]

    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    info = get_data_by_ids(attributes, args["publisher_name"], id_list)

    return jsonify(status=True, data=dumps(info))






