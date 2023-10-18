from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import generics
from rest_framework.response import Response
from rest_framework.views import APIView
from .serializers import *
from .models import *

# Create your views here.
def hello(response):
    return HttpResponse("Hello you")

def index(request):
    return render(request, "index.html")

class WikipageView(generics.CreateAPIView):
    queryset = Wikipage.objects.all()
    serializer_class = WikipageSerializer

class GetRequest(APIView):
    def get(self, request):
        data = {"key1": "value1", "key2": "value2"}
        return Response(data)
    
class PostRequest(APIView):
    def post(self, request):
        data = {request.data['title']: 109}
        return Response(data)