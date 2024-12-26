#coding=utf-8
import logging
from horae.http_helper import JsonHttpResponse
from horae.models import VisitRecord

logger = logging.getLogger('common')

def add_visit_record(func):
    def wrapper(request, *args, **kwargs):
        VisitRecord.request_add(request)
        return func(request, *args, **kwargs)
    return wrapper

def _validate_user_info(request):
    try:
        user_info = request.user.user_info
        user_info.assert_odps_account()
    except Exception as ex:
        return JsonHttpResponse(
            {'status': 1, 'msg': str(ex)})
    return True


def validate_user_info(func):
    def wrapper(request, *args, **kwargs):
        res = _validate_user_info(request)
        if isinstance(res, JsonHttpResponse):
            return res
        return func(request, *args, **kwargs)
    return wrapper


def _validate_params(request, params):
    if params is None:
        return 
    for param in params:
        value = request.POST.get(param, '').strip()
        if not value:
            return JsonHttpResponse(
                {'status': 1, 'msg': '%s参数不能为空' % param})


def validate_params(check_params = None):
    def dec(func):
        def wrapper(request, *args, **kwargs):
            res = _validate_params(request, check_params)
            if isinstance(res, JsonHttpResponse):
                return res
            return func(request, *args, **kwargs)
        return wrapper
    return dec


