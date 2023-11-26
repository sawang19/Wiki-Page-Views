from rest_framework import serializers
from .models import *

class WikipageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Wikipage2301
        fields = '__all__'