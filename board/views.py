from django.shortcuts import render


def trip(request):
    context = {"type": "trip"}
    return render(request, 'board/trip.html', context)


def station(request):
    context = {"type": "station"}
    return render(request, 'board/station.html', context)
