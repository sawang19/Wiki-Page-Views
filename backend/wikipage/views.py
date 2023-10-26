from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import generics
from rest_framework.response import Response
from rest_framework.views import APIView
from .serializers import *
from .models import *
from wikipage.models import Wikipage
import json

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
        result = Wikipage.objects.filter(month='2023-01', keyword='Sa')
        if result.count() == 0:
            return Response("No such record")
        data = {result[0].keyword: result[0].views}
        return Response(data)
    
class PostRequest(APIView):
    def post(self, request):
        keyword = request.data['keyword']
        start_time = request.data['startTime']
        end_time = request.data['endTime']
        months = self.get_months(start_time, end_time)
        views = self.get_views(months, keyword, int(start_time.split('-')[-1]), int(end_time.split('-')[-1]))
        data = {'keyword': keyword, 'views': views}
        return Response(json.dumps(data))
    
    def get_months(self, start_time, end_time):
        months = []
        start_year, start_month, _ = start_time.split('-')
        end_year, end_month, _ = end_time.split('-')
        if (start_year == end_year):
            for month in range(int(start_month), int(end_month) + 1):
                months.append(f"{start_year}-{month:02d}")
            return months
        for year in range(int(start_year), int(end_year) + 1):
            if year == int(start_year):
                for month in range(int(start_month), 13):
                    months.append(f"{year}-{month:02d}")
            elif year == int(end_year):
                for month in range(1, int(end_month) + 1):
                    months.append(f"{year}-{month:02d}")
            else:
                for month in range(1, 13):
                    months.append(f"{year}-{month:02d}")
        return months
    
    def get_views(self, months, keyword, start_date, end_date):
        views = {}
        for month in months:
            results = Wikipage.objects.filter(month=month, keyword=keyword)
            if results.count() == 0:
                return Response("No such record")
            for result in results:
                nums = [int(x) for x in result.views.split("-")]
                if (len(months) == 1):
                    for i in range(start_date - 1, end_date):
                        views[f"{month}-{i + 1:02d}"] = nums[i]
                    return views
                if (month == months[0]):
                    for i in range(start_date - 1, len(nums)):
                        views[f"{month}-{i + 1:02d}"] = nums[i]
                elif (month == months[-1]):
                    for i in range(0, end_date):
                        views[f"{month}-{i + 1:02d}"] = nums[i]
                else:
                    for i in range(0, len(nums)):
                        views[f"{month}-{i + 1:02d}"] = nums[i]
        return views