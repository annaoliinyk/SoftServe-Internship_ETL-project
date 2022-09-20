import logging
import sys

# used "pip install -e "git+https://github.com/openskynetwork/opensky-api.git#egg=opensky-api&subdirectory=python"
# command to install api from GitHub
from src.python.python.opensky_api import OpenSkyApi

# want to see if connection is successful:
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_credentials_from_file():
    file = open("opensky_credentials", "r").read()
    # get username, password from first and second lines of text file:
    username, password = file.split("\n")[0], file.split("\n")[1]
    return username, password


def main():
    username, password = get_credentials_from_file()
    api = OpenSkyApi(username=username, password=password)
    states = api.get_states()
    for s in states.states:
        print("(%r, %r, %r, %r)" % (s.longitude, s.latitude, s.baro_altitude, s.velocity))


if __name__ == "__main__":
    main()
