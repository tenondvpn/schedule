#coding:utf8
from django.db import models
class PermHistory(models.Model):

    resource_type = models.CharField(max_length = 16)
    resource_id = models.BigIntegerField()
    permission = models.CharField(
        max_length = 200,
        verbose_name = 'seprated by |, like: read|update')
    applicant_id = models.IntegerField()
    grantor_id = models.IntegerField()

    (PENDING_STATUS, ACCEPTED_STATUS, DENIED_STATUS, 
     GRANTED_STATUS, REVOKED_STATUS) = range(5)
    status_choices = (
        (PENDING_STATUS, 'pending'),
        (ACCEPTED_STATUS, 'accepted'),
        (DENIED_STATUS, 'denied'),
        (GRANTED_STATUS, 'unsolicited grant'),
        (REVOKED_STATUS, 'unsolicited revoke'),
    )
    status = models.PositiveSmallIntegerField(
        choices = status_choices,
        default = PENDING_STATUS)
    
    update_time = models.DateTimeField(auto_now = True)
    create_time = models.DateTimeField(auto_now_add = True)
    reason = models.CharField(max_length = 512)

