from django.shortcuts import render
from .utils import check_mongo_connection
from django.conf import settings
from django.http import JsonResponse

MONGOUSER = settings.MONGOUSER
MONGOIP = settings.MONGOIP
MONGOPORT = settings.MONGOPORT


def index(request):
    context = {}
    return render(request, 'monitoring.html', context)


def ajax_monitoring_mongo_db(request):
    status, add_info = check_mongo_connection(MONGOIP)
    response = {"status": status, "add_info": add_info or ""}
    return JsonResponse(response)
