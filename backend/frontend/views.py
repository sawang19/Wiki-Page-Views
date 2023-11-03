from django.shortcuts import render

# Create your views here.
def index(request, *args, **kwargs):
    return render(request, 'frontend/index.html')

def event(request, *args, **kwargs):
    return render(request, 'frontend/event.html')

def trend(request, *args, **kwargs):
    return render(request, 'frontend/trend.html')
