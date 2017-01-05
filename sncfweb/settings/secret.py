"""
This module contains configuration information.
"""

import os
import json


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(BASE_DIR, 'secret.json')) as secrets_file:
    secrets = json.load(secrets_file)


def get_secret(setting, mes_Secrets=secrets):
    try:
        return mes_Secrets[setting]
    except KeyError:
        print("Impossible to get " + setting)

# Keep this information secret
MONGOUSER = get_secret('USER')
MONGOIP = get_secret('MONGOIP')
MONGOPORT = int(get_secret('MONGOPORT'))
