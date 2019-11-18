from flask import Flask, jsonify
from cryptography.fernet import Fernet
from pathlib import Path

app = Flask(__name__)

in_memory_key = None


@app.route("/v1/serve_key")
def serve_key():
    global in_memory_key

    if in_memory_key:
        return jsonify(key=in_memory_key.decode())

    elif Path("key.txt").exists():
        with open("key.txt", "rb") as f:
            in_memory_key = f.read()
    else:
        new_key = Fernet.generate_key()

        with open("key.txt", "wb") as f:
            f.write(new_key)

        in_memory_key = new_key

    return jsonify(key=in_memory_key.decode())


if __name__ == '__main__':
    app.run(port=10002)
