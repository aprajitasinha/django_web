from django.shortcuts import render
from gfg_site_app.models import GeeksModel


# Create your views here.

def show_data(request):
    data = GeeksModel.objects.all()
    return render(request, 'data.html', {'data': data})