"""Monitoring views.
"""

from django.shortcuts import render
from monitoring.utils_mongo import check_mongo_connection
from monitoring.utils_dynamo import check_dynamo_connection
from django.http import JsonResponse


def index(request):
    context = {}
    return render(request, 'monitoring.html', context)


def ajax_monitoring_mongo_db(request):
    status, add_info = check_mongo_connection()
    response = {"status": status, "add_info": add_info or ""}
    return JsonResponse(response)


def ajax_monitoring_dynamo_db(request):
    status, add_info = check_dynamo_connection()
    response = {"status": status, "add_info": add_info or ""}
    return JsonResponse(response)
