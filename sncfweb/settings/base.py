# See https://docs.djangoproject.com/en/1.10/howto/deployment/checklist/

import os
from sncfweb.settings.secrets import get_secret
import logging
import sys
from os import path

# Necessary for package loaded as git submodule (in lib folder) to find its imports
sys.path.append("lib")

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))

ENV_DIR = os.path.dirname(BASE_DIR)

SECRET_KEY = get_secret('SECRET_KEY')

ALLOWED_HOSTS = ['*']

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'maps',
    'monitoring',
    'rest_framework',
    'djangobower',
]

MIDDLEWARE = [
    'django.middleware.cache.UpdateCacheMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.cache.FetchFromCacheMiddleware',
]

ROOT_URLCONF = 'sncfweb.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'sncfweb/templates'),
            os.path.join(BASE_DIR, 'maps/templates'),
            os.path.join(BASE_DIR, 'project_api/templates'),
            os.path.join(BASE_DIR, 'documentation/templates'),
            os.path.join(BASE_DIR, 'board/templates'),

        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                #'django.core.context_processors.request',
            ],
        },
    },
]


# Database
# https://docs.djangoproject.com/en/1.10/ref/settings/#databases

DATABASES = {

    'lite': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'mydatabase',
    },

    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': get_secret("RDB_DJANGO_DB_NAME"),
        'USER': get_secret("RDB_USER"),
        'PASSWORD': get_secret("RDB_PASSWORD"),
        'HOST': get_secret("RDB_HOST"),
        'PORT': get_secret("RDB_PORT"),
    },
}


# Password validation
# https://docs.djangoproject.com/en/1.10/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/1.10/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.10/howto/static-files/

STATIC_URL = '/static/'
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, "maps/static"),
    os.path.join(BASE_DIR, "sncfweb/static"),
    os.path.join(BASE_DIR, "monitoring/static"),
    os.path.join(BASE_DIR, "documentation/static"),
    os.path.join(BASE_DIR, "board/static"),
]


STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'djangobower.finders.BowerFinder',
]

BOWER_COMPONENTS_ROOT = path.join(BASE_DIR, 'components')

BOWER_INSTALLED_APPS = (
    'gentelella',
    'd3'
)

LOGS_FILE = os.environ.get('logs_directory', None)
if not LOGS_FILE:
    LOGS_FILE = path.abspath(path.join(BASE_DIR, "..", "logs"))

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s-%(asctime)s-%(module)s-%(message)s'
        },
        'simple': {
            'format': '%(levelname)s-%(message)s'
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': os.path.join(LOGS_FILE, "django.logs"),
            'formatter': 'verbose'
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        'django': {
            'handlers': ['file', 'console'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
            'propagate': False,
        },
    },
}


REST_FRAMEWORK = {
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 20
}

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
        'LOCATION': 'cache_table',
    }
}

CACHE_MIDDLEWARE_SECONDS = 60


# STATIC FILES
# Endroit ou ce sera stocké sur le serveur
# Soit cela est spécifié dans les variables d'environnment, soit on le
# stocke dans un répertoire un niveau au dessus puis dans static
STATIC_ROOT = os.environ.get('STATIC_ROOT', None)
if not STATIC_ROOT:
    STATIC_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..', 'static'))

WSGI_APPLICATION = 'sncfweb.wsgi.application'


# Python crashes or captured as well by logging
def handle_exception(exc_type, exc_value, exc_traceback):
    # if issubclass(exc_type, KeyboardInterrupt):
    #    sys.__excepthook__(exc_type, exc_value, exc_traceback)
    logging.error("Uncaught exception : ", exc_info=(
        exc_type, exc_value, exc_traceback))
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

sys.excepthook = handle_exception
