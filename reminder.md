# Deployment

## Create a Gunicorn systemd Service File
```
sudo nano /etc/systemd/system/gunicorn_sncf.service
```

```
sudo systemctl start gunicorn_sncf
sudo systemctl enable gunicorn_sncf
sudo systemctl status gunicorn_sncf.service

```

## Configure Nginx to Proxy Pass to Gunicorn

```
sudo nano /etc/nginx/sites-available/us_election
```

```
sudo ln -s /etc/nginx/sites-available/sncf_web_project /etc/nginx/sites-enabled
sudo nginx -t
sudo systemctl restart nginx

sudo ufw delete allow 8000
sudo ufw allow 'Nginx Full'
```


## If error:
to see logs:
```
sudo tail -f /var/log/nginx/error.log
sudo systemctl status gunicorn_sncf.service
less /var/log/syslog


```
http://stackoverflow.com/questions/28689445/nginx-django-and-gunicorn-gunicorn-sock-file-is-missing
```
ps auxf | grep gunicorn
grep init: /var/log/syslog

```

To restart
```
sudo systemctl daemon-reload
sudo systemctl start gunicorn_sncf
sudo systemctl enable gunicorn_sncf
sudo systemctl restart nginx
```

# Ssh connection

ssh -i "~/.ssh/aws-eb2" ubuntu@ec2-54-194-138-195.eu-west-1.compute.amazonaws.com

python 3
`pip install -r requirements`

## AWS deploy

```
eb init -p python3.4 sncfweb

eb create sncfweb-env
```

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
