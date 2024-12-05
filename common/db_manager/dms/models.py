#encoding:utf-8
from django.db import models

class Config(models.Model):
    owner_id = models.IntegerField(default=0, db_index=True)
    name = models.CharField(
        max_length = 100,
        unique = True)
    wiki = models.CharField(max_length = 1024, default = '')
    description = models.CharField(max_length = 2048, blank = True, null = True)

    type = models.PositiveSmallIntegerField()
    path = models.CharField(max_length = 1024)
    format = models.CharField(max_length = 2048, blank = True, null = True)
    life_cycle = models.PositiveIntegerField(blank = True, null = True)
    
    generation_cycle = models.CharField(max_length = 10, 
                                        blank = True, null = True)
    
    update_time = models.DateTimeField(auto_now = True)
    update_user_id = models.IntegerField(default=0, db_index=True)

    fluctuation = models.FloatField(default = 0)

    send_mail = models.PositiveSmallIntegerField(default = 1)
    send_sms = models.PositiveSmallIntegerField(default = 0)
