# Generated by Django 2.1 on 2018-09-17 07:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('horae', '0012_rerunhistory'),
    ]

    operations = [
        migrations.AddField(
            model_name='processor',
            name='input_config',
            field=models.TextField(null=True),
        ),
        migrations.AddField(
            model_name='processor',
            name='output_config',
            field=models.TextField(null=True),
        ),
    ]
