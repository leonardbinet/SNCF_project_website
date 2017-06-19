from django.shortcuts import render


def index(request):
    context = {}
    return render(request, 'board/trip.html', context)


def trip(request):
    context = {"type": "trip"}
    return render(request, 'board/trip.html', context)


def station(request):
    context = {"type": "station"}
    return render(request, 'board/station.html', context)
