import json


def get_credentials_from_file():
    with open(r"opensky_credentials.json") as json_file:
        data = json.load(json_file)
        # get username, password from first and second lines of text file:
        username, password = data["username"], data["password"]
        return {"login": username, "password": password}
