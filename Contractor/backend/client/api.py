from flask import Blueprint, jsonify

client_api = Blueprint("client_api", __name__, url_prefix='/client')


@client_api.route("/v1/status")
def status():
    return jsonify(status=True, info="client")
