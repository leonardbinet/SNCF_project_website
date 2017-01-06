from .base import *

import os
import json


DEBUG = True

# BASE_DIR is imported
ENV_DIR = os.path.dirname(BASE_DIR)

# Endroit ou ce sera stocké sur le serveur
STATIC_ROOT = os.path.join(ENV_DIR, 'deploy/static/')
