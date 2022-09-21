import extract_data_from_OpenSkyApi.get_states


def print_states():
    states = get_states.get_states()
    for s in states.states:
        print("(%r, %r, %r, %r)" % (s.longitude, s.latitude, s.baro_altitude, s.velocity))
