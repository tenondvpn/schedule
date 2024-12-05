# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Config',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(unique=True, max_length=100)),
                ('wiki', models.CharField(default=b'', max_length=1024)),
                ('description', models.CharField(max_length=2048, null=True, blank=True)),
                ('type', models.PositiveSmallIntegerField(choices=[(0, b'odps'), (1, b'pangu')])),
                ('path', models.CharField(max_length=1024)),
                ('format', models.CharField(max_length=2048, null=True, blank=True)),
                ('life_cycle', models.PositiveIntegerField(null=True, blank=True)),
                ('generation_cycle', models.CharField(max_length=10, null=True, blank=True)),
                ('update_time', models.DateTimeField(auto_now=True)),
                ('fluctuation', models.FloatField(default=0)),
                ('owner', models.ForeignKey(related_name='data_owner', to=settings.AUTH_USER_MODEL)),
                ('update_user', models.ForeignKey(related_name='data_update_user', to=settings.AUTH_USER_MODEL)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
    ]
