# See https://docs.djangoproject.com/en/1.10/howto/deployment/checklist/

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


def get_secret(setting, my_secrets=secrets):
    try:
        value = my_secrets[setting]
        # set as environment variable
        os.environ[setting] = value
        return my_secrets[setting]
    except KeyError:
        print("Impossible to get " + setting)

SNCF_API_USER = get_secret("SNCF_API_USER")

MONGO_USER = get_secret('MONGO_USER')
MONGO_HOST = get_secret('MONGO_HOST')
MONGO_PASSWORD = get_secret('MONGO_PASSWORD')
# MONGO_PORT = int(get_secret('MONGOPORT'))

SECRET_KEY = get_secret('SECRET_KEY')

POSTRES_HOST = get_secret('POSTRES_HOST')
POSTRES_PORT = get_secret('POSTRES_PORT')
POSTRES_USER = get_secret('POSTRES_USER')
POSTRES_PASSWORD = get_secret('POSTRES_PASSWORD')
POSTRES_DBNAME = get_secret('POSTRES_DBNAME')


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
    'django.contrib.gis',
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
            os.path.join(BASE_DIR, 'maps/templates')
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

WSGI_APPLICATION = 'sncfweb.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.10/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': POSTRES_DBNAME,
        'USER': POSTRES_USER,
        'PASSWORD': POSTRES_PASSWORD,
        'HOST': POSTRES_HOST,
        'PORT': POSTRES_PORT,
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

]

STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'djangobower.finders.BowerFinder',
]

BOWER_COMPONENTS_ROOT = os.path.join(BASE_DIR, 'components')

BOWER_INSTALLED_APPS = (
    'jquery',
    'bootstrap'
)
