from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import generics
from rest_framework.response import Response
from rest_framework.views import APIView
from .serializers import *
from .models import *
from wikipage.models import Wikipage

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
        result = Wikipage.objects.filter(date='2023-01-01', title='Sa_Wang')
        if result.count() == 0:
            return Response("No such record")
        data = {result[0].title: result[0].views}
        return Response(data)
    
class PostRequest(APIView):
    def post(self, request):
        data = {request.data['title']: 109}
        return Response(data)