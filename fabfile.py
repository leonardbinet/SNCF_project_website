from fabric.contrib.files import append, exists, sed
from fabric.api import env, local, run, put
import random
from os import path

env.key_filename = "~/.ssh/aws-eb2"

REPO_URL = 'https://github.com/leonardbinet/SNCF_project_website.git'
PROJECT_NAME = "sncf"
SECRET_PATH = "sncfweb/settings/secret.json"

site_folder = '~/sites/%s' % (PROJECT_NAME)
source_folder = site_folder + '/source'


def deploy():

    _create_directory_structure_if_necessary(site_folder)
    _get_latest_source(source_folder)
    _send_secret_json()
    _update_virtualenv(source_folder)
    _update_static_files(source_folder)
    #_update_database(source_folder)


def _send_secret_json():

    put(SECRET_PATH, path.join(source_folder, SECRET_PATH))


def _create_directory_structure_if_necessary(site_folder):
    for subfolder in ('database', 'static', 'virtualenv', 'source'):
        run('mkdir -p %s/%s' % (site_folder, subfolder))


def _get_latest_source(source_folder):
    if exists(source_folder + '/.git'):
        run('cd %s && git fetch' % (source_folder,))
    else:
        run('git clone %s %s' % (REPO_URL, source_folder))
    current_commit = local("git log -n 1 --format=%H", capture=True)
    run('cd %s && git reset --hard %s' % (source_folder, current_commit))


def _update_virtualenv(source_folder):
    virtualenv_folder = source_folder + '/../virtualenv'
    if not exists(virtualenv_folder + '/bin/pip'):
        run('virtualenv --python=python3 %s' % (virtualenv_folder,))
    run('%s/bin/pip install -r %s/requirements.txt' % (
        virtualenv_folder, source_folder
    ))


def _update_static_files(source_folder):
    run('cd %s && ../virtualenv/bin/python3 manage.py collectstatic --noinput' % (
        source_folder,
    ))


def _update_database(source_folder):
    run('cd %s && ../virtualenv/bin/python3 manage.py migrate --noinput' % (
        source_folder,
    ))
