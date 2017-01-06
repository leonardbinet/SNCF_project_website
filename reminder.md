# Deployment

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
