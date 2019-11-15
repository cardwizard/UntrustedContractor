from requests import get, post

url = "http://localhost:10000/v1/publisher/{}"


def register_client(client_name):
    args = {"publisher_name": client_name}
    response = post(url.format("register_publisher"), data=args)
    print(response.status_code, response.json())


def unregister_client(client_name):
    args = {"publisher_name": client_name}
    response = post(url.format("unregister_publisher"), data=args)
    print(response.status_code, response.json())


if __name__ == '__main__':
    unregister_client("UMD")
