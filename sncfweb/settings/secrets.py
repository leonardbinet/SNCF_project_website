import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))


# SECRETS NOT SAVED IN VCS
try:
    with open(os.path.join(BASE_DIR, 'sncfweb/settings/secret.json')) as secrets_file:
        secrets = json.load(secrets_file)
except:
    secrets = {}
    print("No file")


def get_secret(setting, my_secrets=secrets, env=False):
    try:
        value = my_secrets[setting]
        # set as environment variable
        if env:
            os.environ[setting] = value
        return my_secrets[setting]
    except KeyError:
        print("Impossible to get " + setting)
