import os
from django.core.cache import cache
from django import template
import dags.settings
 
register = template.Library()
@register.filter(name = "file_time_stamp")
def file_time_stamp(value):
    cache_key = "_file_time_stamp__%s" % value
    v = cache.get(cache_key)
    if v: 
        return v

    if value.startswith("/"):
        fn = os.path.join(dags.settings.BASE_DIR, value[1:].replace("/", os.sep))
        if os.path.isfile(fn):
            ts = os.stat(fn).st_mtime
            sp = "?" if "?" not in value else "&"
            value = "%s%sfmts=%.1f" % (value, sp, ts)
    cache.set(cache_key, value, 300)
    return value
