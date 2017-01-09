# Deployment

# Ssh connection

ssh -i "/Users/leonardbinet/.ssh/aws-eb2" ec2-user@ec2-54-154-171-111.eu-west-1.compute.amazonaws.com

python 3
`pip install -r requirements`

## AWS deploy

```
eb init -p python3.4 sncfweb

eb create sncfweb-env
```
For security reasons, secret files on not known by the VCS. So when deploying my app with AWS EB, I need to get the secret file from a S3 instance.
http://docs.aws.amazon.com/elasticbeanstalk/latest/dg/https-storingprivatekeys.html

More information:
https://realpython.com/blog/python/deploying-a-django-app-to-aws-elastic-beanstalk/

# API credentials
SNCF API

TRANSILIEN API

NAVITIA API

# Databases
Needs one Postrgres and one Mongo.

## Postrgres requirements
You have to set up a Postrgres DB, and either:
- set environments variables for connection:
    - POSTRES_HOST
    - POSTRES_PORT
    - POSTRES_USER
    - POSTRES_DBNAME
    - POSTRES_PASSWORD

- OR write these variables in a JSON file in sncfweb/settings/secret.json:

# Python 3
sudo curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py
pip3 install --user -r requirements.txt

## Secrets


ssh -i "/Users/leonardbinet/.ssh/aws-eb2" ec2-user@ec2-54-154-171-111.eu-west-1.compute.amazonaws.com

sudo chmod 777 settings

scp -i "/Users/leonardbinet/.ssh/aws-eb2" secret.json ec2-user@ec2-54-154-171-111.eu-west-1.compute.amazonaws.com:


sudo mv secret.json /opt/python/current/app/sncfweb/settings
sudo chmod 777 secret.json

## Wsgi
sudo chmod a+x wsgi.py

## Mysql
```
pip install --egg http://dev.mysql.com/get/Downloads/Connector-Python/mysql-connector-python-2.1.4.zip
```
brew install mysql-connector-c


## MongoDB requirements

For french map: you need to insert file: "fr_stop_points.geojson" in 'stop_points' collection in 'sncf' database.

All other collections will be automatically created.

```
from pymongo import MongoClient, GEOSPHERE
import json

client = MongoClient("mongodb://<user>:<password>@<host>:<port>")

db = client("sncf")
collection = db["stop_points"]

with open("fr_stop_points.geojson", encoding="utf-8-sig") as jsonfile:                        
    stations = json.load(jsonfile)

collection.insert_many(stations["features"])

# Create sphere index:
collection.create_index([("geometry", GEOSPHERE)])

```


# Env creation

pandas installation:
http://stackoverflow.com/questions/29516084/gcc-failed-during-pandas-build-on-aws-elastic-beanstalk
http://stackoverflow.com/questions/11094718/error-command-gcc-failed-with-exit-status-1-while-installing-eventlet


https://github.com/ashokfernandez/Django-Fabric-AWS

https://www.digitalocean.com/community/tutorials/how-to-set-up-django-with-postgres-nginx-and-gunicorn-on-centos-7

http://agiliq.com/blog/2014/08/deploying-a-django-app-on-amazon-ec2-instance/
http://stackoverflow.com/questions/16123459/virtualenvwrapper-and-python-3
http://ask.xmodulo.com/install-python3-centos.html
```
sudo yum install python-devel postgresql-devel


sudo yum install nginx
sudo service nginx start

sudo vim /etc/nginx/sites-enabled/default

sudo pip install gunicorn
```
open HTTP traffic on aws for this instance
