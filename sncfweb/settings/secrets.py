import os
from os import path
import json
import logging

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

try:
    with open(path.join(BASE_DIR, 'secret.json')) as secrets_file:
        secrets = json.load(secrets_file)
except FileNotFoundError:
    secrets = {}
    logger.info("No file")


def get_secret(setting, my_secrets=secrets, env=True):
    """
    Tries to find secrets either in secret file, or in environment variables.
    env > secret file
    Then, set it as environment variable and returns value.
    """
    value = None
    # Try to get value from env then from file

    try:
        value = my_secrets[setting]
        if env:
            os.environ[setting] = value
        return value
    except KeyError:
        logger.debug("Impossible to get %s from file" % setting)

    try:
        value = os.environ[setting]
        return value
    except KeyError:
        logger.debug("Impossible to get %s from environment" % setting)

    logger.info("Impossible to find %s." % setting)

