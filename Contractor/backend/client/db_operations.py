from pathlib import Path
from json import load


def get_local_schema(client_name: str, table_name: str, projection_name):
    path = Path("Projections").joinpath(client_name).joinpath(table_name).joinpath(
        "projection_{}.json".format(projection_name))

    if not path.exists():
        return None

    with open(path, "r") as f:
        data = load(f)

    return data