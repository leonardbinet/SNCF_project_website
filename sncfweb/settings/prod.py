from .base import *

import os
import json

DEBUG = False

S3_STATIC_PATH = ""

# endroit ou ce sera stocké sur le serveur
STATIC_ROOT = os.path.join(S3_STATIC_PATH, 'static/')
