# API Documentation

## Overview


This documentation describes how to use the SNCF API based on Navitia software. Navitia is an Open Source software developed by Kisio Digital. To see the last update, please go to navitia.io documentation (http://doc.navitia.io/).

**The SNCF API contains**
- theoretical train data for the following commercial modes: TGV, TER, Transilien, Intercités.
- realtime train data for the following commercial modes: TGV, TER, Intercités.

**The SNCF API handles:**
- Journeys computation
- Line schedules
- Next departures
- Exploration of public transport data/ search places
- Autocomplete Isochrones

Read the Open Transport vocabulary (https://github.com/OpenTransport/vocabulary/blob/master/vocabulary.md).

## Authentication

You must authenticate to use SNCF API. When you register we give you an authentication key to the API. You must use the Basic HTTP authentication, where the username is the key, and without password. Username: copy / paste your key Password: leave the field blank

## Easy examples

Easy executable examples are available on JSFiddle

**JOURNEYS feature**
http://jsfiddle.net/gh/get/jquery/2.2.2/SNCFdevelopers/API-trains-sncf/tree/source/examples/jsFiddle/journeys/

**ISOCHRONES**
http://jsfiddle.net/gh/get/jquery/2.2.2/SNCFdevelopers/API-trains-sncf/tree/source/examples/jsFiddle/isochron/

**LINES feature**
http://jsfiddle.net/gh/get/jquery/2.2.2/SNCFdevelopers/API-trains-sncf/tree/source/examples/jsFiddle/lines/


## Training
http://canaltp.github.io/navitia-playground/play.html

http://canaltp.github.io/navitia-playground/play.html?request=https%3A%2F%2Fapi.navitia.io%2Fv1%2Fcoverage%2Fsandbox%2Flines&token=3b036afe-0110-4202-b9ed-99718476c2e0


## Full documentation
http://doc.navitia.io/
