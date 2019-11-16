from flask import Blueprint, jsonify
from flask_restful import reqparse
from json import loads, dumps
from Contractor.backend.publisher.db_operations import get_data, build_schema


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