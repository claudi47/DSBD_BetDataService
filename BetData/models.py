from django.db import models

# In Django, a model defines the structure of DB table
# models.py is where we define our database models which Django automatically transaltes into database tables
# Create your models here.
# class User(models.Model):
#     # models is a package, <Type>Field indicates the type of field
#     username = models.CharField(max_length=127)
#     user_identifier = models.CharField(max_length=127, primary_key=True)
#
#     max_research = models.IntegerField(default=-1)
#     ban_period = models.DateTimeField(null=True)
#
#     timestamp = models.DateTimeField(auto_now_add=True)


class Search(models.Model):
    # the 'blank' option is used to specify that, initially, the field can be empty
    csv_url = models.CharField(max_length=255, blank=True)
    web_site = models.CharField(max_length=127)
    timestamp = models.DateTimeField(auto_now_add=True)

    # the related_name represents the Primary Key for the one-to-many relation
    user = models.ForeignKey(User, related_name='searches', on_delete=models.CASCADE)


class BetData(models.Model):
    date = models.CharField(max_length=127)
    match = models.CharField(max_length=127)
    one = models.CharField(max_length=127, blank=True)
    ics = models.CharField(max_length=127, blank=True)
    two = models.CharField(max_length=127, blank=True)
    gol = models.CharField(max_length=127, blank=True)
    over = models.CharField(max_length=127, blank=True)
    under = models.CharField(max_length=127, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    search = models.ForeignKey(Search, related_name='bet_data', on_delete=models.CASCADE)

class Settings(models.Model):
    goldbet_research = models.BooleanField(default=True)
    bwin_research = models.BooleanField(default=True)
    