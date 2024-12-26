#coding=utf-8
import logging
import traceback

from django.http import *
from django.conf import settings
from django.template import RequestContext, loader 

import json

logger = logging.getLogger(settings.PROJECT_NAME)

def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip

class JsonHttpResponse(HttpResponse):
    '''
    json格式的response
    ret:返回结果
    '''
    def __init__(self,ret):
        content_type = "application/json"
        try:
            ret = json.dumps(ret, ensure_ascii=False)
            HttpResponse.__init__(self,ret,content_type)
        except Exception as ex:
            logger.error("construct json http response failed!ret<%s>,msg<%s>" % (ret, traceback.format_exc()))
            ret = {'status':1,'msg':'系统出错，请联系管理员！'}
            ret = json.dumps(ret)
            HttpResponse.__init__(self,ret,content_type)
        

class RequestContextResponse(HttpResponse):
    '''
    返回包含request context处理的response
    '''
    def __init__(self,request,template_file):
        t = loader.get_template(template_file)
        c = RequestContext(request)
        HttpResponse.__init__(self,t.render(c),content_type="text/html")

                
