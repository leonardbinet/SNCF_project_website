from fabric.contrib.files import append, exists, sed
from fabric.api import env, local, run, put, sudo
from fabric.api import settings
import random
from os import path

env.key_filename = "~/.ssh/aws-eb2"
# fab deploy:host=ubuntu@ec2-54-194-138-195.eu-west-1.compute.amazonaws.com

REPO_URL = 'https://github.com/leonardbinet/SNCF_project_website.git'
PROJECT_NAME = "sncf"
SECRET_PATH = "sncfweb/settings/secret.json"

gunicorn_file_name = "gunicorn_sncf.service"
nginx_file_name = "sncf"

# Do not touch
site_folder = '~/sites/%s' % (PROJECT_NAME)
source_folder = path.join(site_folder, 'source')
deploy_folder = path.join(source_folder, 'deploy')

gunicorn_remote_path = path.join("/etc/systemd/system/", gunicorn_file_name)
gunicorn_local_path = path.join(deploy_folder, gunicorn_file_name)

nginx_remote_path = path.join("/etc/nginx/sites-available", nginx_file_name)
nginx_local_path = path.join(deploy_folder, nginx_file_name)


def deploy():

    _create_directory_structure_if_necessary(site_folder)
    _get_latest_source(source_folder)
    _send_secret_json()
    _update_virtualenv(source_folder)
    _update_static_files(source_folder)
    _set_gunicorn_service()
    _set_nginx_service()
    _restart_all()
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


def _set_gunicorn_service():
    # first create gunicorn service file
    sudo("mv %s %s" % (gunicorn_local_path, gunicorn_remote_path))
    # then activate it
    sudo("systemctl start %s" % gunicorn_file_name.split(".")[0])
    sudo("systemctl enable %s" % gunicorn_file_name.split(".")[0])


def _set_nginx_service():
    # install nginx: already done
    # create path:
    # sudo('mkdir -p %s' % "/etc/nginx/sites-available")
    # create service
    sudo("mv %s /etc/nginx/sites-available" % nginx_file_name)
    # activate it with symbolic link (because it is present)
    with settings(warn_only=True):
        sudo("ln -s /etc/nginx/sites-available/%s /etc/nginx/sites-enabled" %
             nginx_file_name)
    # remove default server nginx:
    with settings(warn_only=True):
        sudo("rm /etc/nginx/sites-enabled/default")
    # restart server:
    sudo("systemctl restart nginx")


def _restart_all():
    sudo("systemctl daemon-reload")
    sudo("systemctl start " + gunicorn_file_name.split(".")[0])
    sudo("systemctl enable " + gunicorn_file_name.split(".")[0])
    sudo("systemctl restart nginx")
