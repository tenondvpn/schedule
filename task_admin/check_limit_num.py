###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import threading
import sys
import logging
import copy

import admin_sql_manager
sys.path.append('../common')
import task_util

class CheckLimitNum(threading.Thread):
    """
        安全管理全局任务流程图，检查任务数限制
    """
    
    def __init__(self):
        if not hasattr(self, "a"):
            threading.Thread.__init__(self)
            self.__log = logging
            self.__sql_manager = admin_sql_manager.SqlManager()

            self.__graph_lock = threading.Lock()
            self.__edge_map = {}
            self.__task_map = {}
            self.__pipeline_map = {}
            self.__sub_graph_list = []
            self.__graph = None
            self.__limit_lock = threading.Lock()
            self.__owner_task_num_map = {}
            self.__type_task_num_map = {}

            self.a = "a"  # 必须在后面

    def __new__(cls, *args, **kw):
        if not hasattr(cls, "_instance"):
            orig = super(CheckLimitNum, cls)
            cls._instance = orig.__new__(cls, *args, **kw)

        return cls._instance

    def run(self):
        self.__log.info("CheckLimitNum thread starting...")
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            # admin是否获取了zookeeper锁，具有处理数据的权限
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                time.sleep(1)
                continue

            begin_time = time.time()
            self.__log.info("CheckLimitNum handle data starting...")
            while True:
                if not self.__get_task_limit_num_from_db():
                    self.__log.error("get task_limit info from db failed!")
                    break
                break
            use_time = time.time() - begin_time
            self.__log.info("CheckLimitNum handle data "
                    "exit.use time[%f]" % use_time)
            time.sleep(10)  # 失败后任然需要sleep
        self.__log.info("CheckLimitNum thread existed!")

    def check_limit_num_can_run(self, owner_id, task_type):
        # 数量限制放到node进行检查
        return True

    def get_task_info_copy(self):
        # task_util.AutoLock(self.__graph_lock)
        self.__graph_lock.acquire()
        # 一定要返回拷贝
        try:
            return  self.__edge_map.copy(), \
                    self.__task_map.copy(), \
                    self.__pipeline_map.copy(), \
                    self.__sub_graph_list[:], \
                    copy.deepcopy(self.__graph)
        except:
            pass
        finally:
            self.__graph_lock.release()

    def set_task_info_copy(
            self, 
            edge_map, 
            task_map, 
            pipeline_map, 
            sub_graph_list, 
            graph):
        # task_util.AutoLock(self.__graph_lock)
        self.__graph_lock.acquire()
        # 一定要copy
        self.__edge_map = edge_map.copy()
        self.__task_map = task_map.copy()
        self.__pipeline_map = pipeline_map.copy()
        self.__sub_graph_list = sub_graph_list[:]
        self.__graph = copy.deepcopy(graph)
        self.__graph_lock.release()

    def __get_task_limit_num_from_db(self):
        owner_task_num_map = {}
        type_task_num_map = {}

        type_tasks = self.__sql_manager.get_task_num_with_group_type()
        if type_tasks is None:
            self.__log.error("get_task_num_with_group_type failed!") 
            return False

        for task in type_tasks:
            type_task_num_map[int(task['type'])] = task['sum_type']

        owner_type_tasks = self.__sql_manager.get_owner_task_num_with_type()
        if owner_type_tasks is None:
            self.__log.error("get_owner_task_num_with_type failed!")
            return False

        for task in owner_type_tasks:
            tmp_map = {}
            tmp_map[int(task['owner_id'])] = task['sum_type_owener']
            owner_task_num_map[int(task['type'])] = tmp_map
        # task_util.AutoLock(self.__limit_lock)
        self.__limit_lock.acquire()
        self.__owner_task_num_map = owner_task_num_map
        self.__type_task_num_map = type_task_num_map
        self.__limit_lock.release()
        return True
    
