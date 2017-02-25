# See https://docs.djangoproject.com/en/1.10/howto/deployment/checklist/

import os
import json
from .secrets import get_secret

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))

ENV_DIR = os.path.dirname(BASE_DIR)


SNCF_API_USER = get_secret("SNCF_API_USER")

MONGO_USER = get_secret('MONGO_USER')
MONGO_HOST = get_secret('MONGO_HOST')
MONGO_PASSWORD = get_secret('MONGO_PASSWORD')
# MONGO_PORT = int(get_secret('MONGOPORT'))

SECRET_KEY = get_secret('SECRET_KEY')

DJANGO_DB_HOST = get_secret('DJANGO_DB_HOST')
DJANGO_DB_PORT = get_secret('DJANGO_DB_PORT')
DJANGO_DB_USER = get_secret('DJANGO_DB_USER')
DJANGO_DB_PASSWORD = get_secret('DJANGO_DB_PASSWORD')
DJANGO_DB_NAME = get_secret('DJANGO_DB_NAME')


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
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'sncfweb.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'sncfweb/templates'),
            os.path.join(BASE_DIR, 'maps/templates'),
            os.path.join(BASE_DIR, 'project_api/templates'),
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

    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'mydatabase',
    },

    #'other': {
    #    'ENGINE': 'django.db.backends.mysql',
    #    'NAME': DJANGO_DB_NAME,
    #    'USER': DJANGO_DB_USER,
    #    'PASSWORD': DJANGO_DB_PASSWORD,
    #    'PORT': DJANGO_DB_PORT,
    #},
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

]

# Endroit ou ce sera stocké sur le serveur
# Soit cela est spécifié dans les variables d'environnment, soit on le
# stocke dans un répertoire un niveau au dessus puis dans static
STATIC_ROOT = os.environ.get('static', None)
if not STATIC_ROOT:
    STATIC_ROOT = os.path.abspath(os.path.join(BASE_DIR, '../static'))

STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'djangobower.finders.BowerFinder',
]

BOWER_COMPONENTS_ROOT = os.path.join(BASE_DIR, 'components')

BOWER_INSTALLED_APPS = (
    'jquery',
    'bootstrap',
    'BlackrockDigital/startbootstrap-sb-admin-2'
)

LOGS_FILE = "../logs"

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
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
            'propagate': True,
        },
    },
}
