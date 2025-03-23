from django.shortcuts import render

# Create your views here.

"""def show_data(request):
    data = GeeksModel.objects.all()
    return render(request, 'data.html', {'data': data})"""


def homepage(request):
    return render(request, 'homepage.html')  # This loads from templates/