#coding=utf-8
import logging
from django.db import models
from django.conf import settings
from common.ark_time import *
from common.odps import Odps
from common.pangu import Pangu
from odps_data import OdpsData

logger = logging.getLogger(settings.PROJECT_NAME)

class ConfigManager(models.Manager):
    #创建数据
    def create_config(self,form,user):
        try:
            config = form.save(commit=False) 
            config.owner = user
            config.update_user = user
            config.save()
        except Exception as ex:
            logger.error('create data fail: <%s>' % str(ex))
            #raise Exception('create data fail')
            raise Exception('create data fail: <%s>' % str(ex))
            # raise Exception(str(ex))

    #修改数据
    def update_config(self,form,user):
        try:
            config = form.save(commit=False) 
            config.update_user = user
            config.save()
        except Exception as ex:
            logger.error('create data fail: <%s>' % str(ex))
            #raise Exception('create data fail')
            raise Exception('create data fail: <%s>' % str(ex))
            # raise Exception(str(ex))

    #删除数据
    def delete_config(self,id = None):
        try:
            config = self.get(id=id)
            config.delete()
        except Exception as ex:
            logger.error('delete data fail: <%s>' % str(ex))
            raise Exception('delete data fail: <%s>' % str(ex))

    def get_monitor_list(self):
        return self.exclude(generation_cycle = '')

        
    def get_size(self, config, data_time):
        path = ArkTime.get_real_path(config.path, data_time)
        try:
            if config.type == self.model.TYPE_ODPS:
                project, table_name, partition = OdpsData.parse_path(path)
                return Odps.size(table_name, partition)
            elif config.type == self.model.TYPE_PANGU:
                return Pangu.size(path)
        except Exception as ex:
            logger.error(str(ex))
            return self.model.SIZE_NOT_FOUND



