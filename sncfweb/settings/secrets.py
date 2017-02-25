import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))


# SECRETS NOT SAVED IN VCS
try:
    with open(os.path.join(BASE_DIR, 'secret.json')) as secrets_file:
        secrets = json.load(secrets_file)
except:
    secrets = {}
    print("No file")


def get_secret(setting, my_secrets=secrets, env=True):
    """
    Tries to find secrets either in secret file, or in environment variables. Secret file > env
    Then, set it as environment variable and returns value.
    """
    value = None
    try:
        value = os.environ[setting]
    except KeyError:
        print("Impossible to get %s from environment" % setting)
    if value:
        os.environ[setting] = value
        return value

    try:
        value = my_secrets[setting]
    except KeyError:
        print("Impossible to get %s from file" % setting)

    if value:
        os.environ[setting] = value
        return value
    else:
        print("%s not found." % setting)
