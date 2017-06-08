from django.shortcuts import render
import pandas as pd


def index(request):
    context = {}
    return render(request, 'board/index.html', context)


def trip(request):
    context = {"type": "trip"}
    return render(request, 'board/index.html', context)


def station(request):
    """This view shows two things:
    \n- a table describing possible stations
    """
    gares = pd.read_csv(
        "data/sncf-gares-et-arrets-transilien-ile-de-france.csv", sep=";")
    cols = [
        'Code UIC', 'uic7', "Libelle point d'arret", 'Libelle',
        'Libelle STIF (info voyageurs)', 'Libelle SMS gare', 'Nom Gare',
        'Adresse', 'Code INSEE commune', 'Commune', 'X (Lambert II etendu)',
        'Y (Lambert II etendu)', 'Coord GPS (WGS84)', 'Zone Navigo',
        'Gare non SNCF'
    ]
    cols_keep = [
        'Code UIC', 'uic7', "Libelle point d'arret", 'Zone Navigo',
        'Gare non SNCF'
    ]
    gares = gares[cols_keep].applymap(str)
    gares = gares.to_dict(orient="record")
    context = {"type": "station"}
    return render(request, 'board/station.html', context)
