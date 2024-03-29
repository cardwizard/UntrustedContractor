from flask import Blueprint, jsonify
from flask_restful import reqparse
from Contractor.backend.publisher.db_operations import *
from json import loads, dumps

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


@publisher_api.route("/define_new_table", methods=["POST"])
def define_table():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)

    args = parser.parse_args()

    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    stat = create_table(attributes, args["publisher_name"])
    return jsonify(status=stat)


@publisher_api.route("/drop_table", methods=["POST"])
def drop_table_():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)

    args = parser.parse_args()
    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    stat = drop_table(attributes, args["publisher_name"])
    return jsonify(status=stat)


@publisher_api.route("/add_data", methods=["POST"])
def add_data_():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("alchemy_schema", type=loads)
    parser.add_argument("data", type=loads)

    args = parser.parse_args()
    attributes = build_schema(args["table_name"], args["alchemy_schema"])
    stat = push_data(attributes, args["publisher_name"], args["data"])

    return jsonify(status=stat)


@publisher_api.route("/add_projection", methods=["POST"])
def add_projection_():
    parser = reqparse.RequestParser()
    parser.add_argument("publisher_name", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("column", type=str)
    parser.add_argument("schema", type=loads)
    parser.add_argument("data", type=loads)
    parser.add_argument("projection_type", type=str, default="normal")

    args = parser.parse_args()
    proj_suffix = "projection_{}" if args["projection_type"] == "normal" else "projection_agg_{}"

    attributes = build_schema(proj_suffix.format(args["column"]), args["schema"])
    create_table(attributes, args["publisher_name"])

    attributes = build_schema(proj_suffix.format(args["column"]), args["schema"])

    cache_schema_locally(args["publisher_name"], args["table_name"], args["column"], args["schema"], proj_suffix)
    stat = push_data(attributes, args["publisher_name"], args["data"])
    return jsonify(status=stat, message="Table creation_successful")
