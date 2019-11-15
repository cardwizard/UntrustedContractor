from flask import Blueprint, jsonify
from flask_restful import reqparse
from Contractor.backend.publisher.db_operations import create_db, drop_db, build_schema, create_table
from json import loads

publisher_api = Blueprint("publisher_api", __name__, url_prefix='/v1/publisher')


@publisher_api.route("/status")
def status():
    return jsonify(status=True, info="publisher")


@publisher_api.route("/register_publisher", methods=["POST"])
def register():
    parser = reqparse.RequestParser()
    parser.add_argument('publisher_name', type=str)
    args = parser.parse_args()

    stat = create_db(args["publisher_name"])
    return jsonify(status=stat)


@publisher_api.route("/unregister_publisher", methods=["POST"])
def unregister():
    parser = reqparse.RequestParser()
    parser.add_argument('publisher_name', type=str)
    args = parser.parse_args()

    stat = drop_db(args["publisher_name"])
    return jsonify(status=stat)


@publisher_api.route("/add_new_table", methods=["POST"])
def add_schema():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)

    args = parser.parse_args()
    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    stat = create_table(attributes, args["publisher_name"])
    return jsonify(status=stat)
