from .base import *
from .base import BASE_DIR

import os

DEBUG = True

# Endroit ou ce sera stocké sur le serveur
# Soit cela est spécifié dans les variables d'environnment, soit on le
# stocke dans un répertoire un niveau au dessus puis dans static
STATIC_ROOT = os.environ.get('static', None)
if not STATIC_ROOT:
    STATIC_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..', 'static'))
