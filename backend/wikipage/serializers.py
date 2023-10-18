from rest_framework import serializers
from .models import *

class WikipageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Wikipage
        fields = '__all__'