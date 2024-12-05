###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import os,sys
import logging
import logging.config

import task_util

class LogManager(task_util.Singleton):
    """
        初始化日志配置信息
    """
    def __init__(self, conf_path=''):
        if not hasattr(self, "a"):
            self.__init(conf_path)
            self.a = "a"  # 必须在__init后面

    def __init(self, conf_path=''):
        if conf_path.strip() == '':
            raise ValueError("db conf path is null!");

        logging.config.fileConfig(conf_path)
        self.root_log = logging.getLogger("root")
        self.httpserver_log = logging.getLogger("httpserver")
        self.monitors_log = logging.getLogger("monitors")
        self.root_log = logging.getLogger("taskinit")
        self.schedule_log = logging.getLogger("schedule")
        self.db_log = logging.getLogger("dblog")
        self.dtschedule_log= logging.getLogger("dtschedule")
        self.userinfo_log= logging.getLogger("userinfoutil")
        self.zookeeper_log= logging.getLogger("Zookeeper")

