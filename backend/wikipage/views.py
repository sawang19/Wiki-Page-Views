from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import generics
from rest_framework.response import Response
from rest_framework.views import APIView
# from .serializers import *
# from .models import *
from wikipage.models import *
import json
import calendar

# Create your views here.
def index(request):
    return render(request, "index.html")
    
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
            tot_days = self.days_in_month(month)
            if len(months) == 1:
                date_range = range(start_date, end_date + 1)
            elif month == months[0]:
                date_range = range(start_date, tot_days + 1)
            elif month == months[-1]:
                date_range = range(1, end_date + 1)
            else:
                date_range = range(1, tot_days + 1)

            for day in date_range:
                key = f"{month}-{day:02d}"
                views[key] = 0

        for month in months:
            # if month == '2023-01':
            month_part = month.split('-')[1]
            results = monthly_models[month_part].objects.filter(keyword=keyword)
            for result in results:
                nums = [x.split(":") for x in result.views.split("-") if len(x.split(":")) == 2]
                for day, view in nums:
                    key = f"{month}-{int(day):02d}"
                    if key in views: 
                        views[key] += int(view)

        return dict(sorted(views.items()))

    
    def days_in_month(self, s):
        year, month = map(int, s.split('-'))
        return calendar.monthrange(year, month)[1]
    
class Top10Request(APIView):
    def post(self, request):
        date = request.data['date']
        top10 = wikitop10.objects.filter(date=date)

        titles = []
        views = []
        for item in top10:
            titles.extend([item.title_1, item.title_2, item.title_3, item.title_4, item.title_5, 
                           item.title_6, item.title_7, item.title_8, item.title_9, item.title_10])

            views.extend([item.views_1, item.views_2, item.views_3, item.views_4, item.views_5, 
                          item.views_6, item.views_7, item.views_8, item.views_9, item.views_10])

        response_data = {
            'titles': titles,
            'views': views,
        }
        return Response(json.dumps(response_data))

