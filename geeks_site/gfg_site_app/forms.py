
from gfg_site_app.models import GeeksModel

obj = GeeksModel.objects.get(id=1)
obj.title = "GFG"
obj.save()



GeeksModel.objects.all()

for obj in GeeksModel.objects.all():
    print(obj.title, obj.description)