import os
from django.shortcuts import render
from sncfweb.utils_mongo import check_mongo_connection
from django.http import JsonResponse

MONGO_USER = os.environ["MONGO_USER"]
MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PASSWORD = os.environ["MONGO_PASSWORD"]


def index(request):
    context = {}
    return render(request, 'monitoring.html', context)


def ajax_monitoring_mongo_db(request):
    status, add_info = check_mongo_connection(
        user=MONGO_USER, host=MONGO_HOST, password=MONGO_PASSWORD)
    response = {"status": status, "add_info": add_info or ""}
    return JsonResponse(response)
