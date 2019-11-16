from requests import get


def get_key() -> str:
    response = get("http://localhost:10002/v1/serve_key")
    return response.json()["key"]
