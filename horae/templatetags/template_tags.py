#encoding:utf-8
import logging
import json
import traceback
from django.conf import settings
logger = logging.getLogger(settings.PROJECT_NAME)
from common.util import is_admin

from django import template
register = template.Library()

@register.simple_tag
def user_is_admin(user):
    result = is_admin(user)
    return result

@register.simple_tag
def user_display(user):
    dsp = user.username
    try:
        info = user.user_info
        if info.ali_name:
            dsp = info.ali_name
    except:
        pass
    return dsp

@register.simple_tag
def message_count(user):
    return 1

@register.simple_tag
def system_header_urls():
    value = ""
    urls = []
    for item in value.split('\n'):
        item = item.strip()
        parts = item.split()
        if len(parts) != 2:
            logger.error('invalid sys_header_urls config, %s' % item)
        else:
            urls.append([parts[0], parts[1],])
    return urls
                        

@register.simple_tag
def home_sys_introductions():
    infos = []
    value = ""
    try:
        infos = json.loads(value)
    except:
        logger.error('parse home_sys_introductions fail, %s' % traceback.format_exc())

    return infos

                        
@register.simple_tag
def ark_main_site_host(user):
    value = ""
    return value









