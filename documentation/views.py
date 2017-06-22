from django.shortcuts import render


def documentation(request):
    context = {}
    return render(request, 'documentation.html', context)

def source_code(request):
    context = {}
    return render(request, 'source_code.html', context)