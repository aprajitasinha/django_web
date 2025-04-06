from django.db import models

# Create your models here.
class GeeksModel(models.Model):
        # fields of the model
    title = models.CharField(max_length=200)
    description = models.TextField()
    last_modified = models.DateTimeField(auto_now_add=True)
    img = models.ImageField(upload_to="images/") 
    


    # renames the instances of the model
    # with their title name
    def __str__(self):
        return self.title
    
    
    

class CandlestickData(models.Model):
    timestamp = models.BigIntegerField(primary_key=True)
    open_price = models.FloatField()
    high_price = models.FloatField()
    low_price = models.FloatField()
    close_price = models.FloatField()
    volume = models.IntegerField()
