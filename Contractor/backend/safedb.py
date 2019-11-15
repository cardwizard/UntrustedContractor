from flask import Flask
from Contractor.backend.publisher.api import publisher_api
from Contractor.backend.client.api import client_api

app = Flask(__name__)
app.register_blueprint(publisher_api)
app.register_blueprint(client_api)
