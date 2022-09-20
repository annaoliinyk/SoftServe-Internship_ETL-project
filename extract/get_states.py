# used "pip install -e "git+https://github.com/openskynetwork/opensky-api.git#egg=opensky-api&subdirectory=python"
# command to install api from GitHub
from src.python.python.opensky_api import OpenSkyApi


def get_states():
    api = OpenSkyApi()
    s = api.get_states()
    print(s)


get_states()
