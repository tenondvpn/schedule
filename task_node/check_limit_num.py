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
import traceback

import node_sql_manager
sys.path.append('../common')
import task_util

class CheckLimitNum(threading.Thread):
    """
        安全管理全局任务流程图，检查任务数限制
    """
    
    def __init__(self):
        if not hasattr(self, "a"):
            self.a = "a"  # 必须在后面
            threading.Thread.__init__(self)
            self.__log = logging
            self.__sql_manager = node_sql_manager.SqlManager()

            self.__task_map = {}
            self.__pipeline_map = {}
            self.__sub_graph_list = []
            self.__graph = None
            self.__limit_lock = threading.Lock()
            self.__owner_task_num_map = {}
            self.__type_task_num_map = {}

            self.__wait_running_type_map = {}
            self.__wait_running_owner_map = {}
            self.__test_time = time.time() * 1000000
            self.__log.info("success create CheckLimitNum")

    def __new__(cls, *args, **kw):
        if not hasattr(cls, "_instance"):
            orig = super(CheckLimitNum, cls)
            cls._instance = orig.__new__(cls, *args, **kw)

        return cls._instance

    def run(self):
        self.__log.info("CheckLimitNum thread starting...")
        # 全局退出标示字段
        log_begin_time = time.time()
        while not task_util.CONSTANTS.GLOBAL_STOP:
            begin_time = time.time()
            if time.time() - log_begin_time >= 60:
                self.__log.info("CheckLimitNum handle data starting...")

            try:
                if not self.__get_task_limit_num_from_db():
                    self.__log.error("get task_limit info from db failed!")
            except Exception as ex:
                self.__log.error("handle request failed![ex:%s][trace: %s]" % (
                        str(ex), traceback.format_exc()))

            if time.time() - log_begin_time >= 60:
                use_time = time.time() - begin_time
                self.__log.info("CheckLimitNum handle data "
                        "exit.use time[%f]" % use_time)
                log_begin_time = time.time()
            time.sleep(1)  # 失败后任然需要sleep
        self.__log.info("CheckLimitNum thread existed!")

    def check_limit_num_can_run(self, owner_id, task_type):
        owner_id = int(owner_id)
        task_type = int(task_type)
        if task_type not in task_util.global_task_limit_map:
            self.__log.error("task type[%d] unkown![task_id:%s" % \
                    (task_type, owner_id))
            return False

        local_lock = task_util.AutoLock(self.__limit_lock)
        try:
            if task_type not in self.__owner_task_num_map:
                tmp_map = {}
                tmp_map[owner_id] = 0
                self.__owner_task_num_map[task_type] = tmp_map

            if owner_id not in self.__owner_task_num_map[task_type]:
                self.__owner_task_num_map[task_type][owner_id] = 0

            self.__add_wait_running_limit_num(owner_id, task_type)
            type_num, owner_num = self.__get_wait_runnint_num(
                    owner_id, 
                    task_type)
            # 任务类型数是否超过了限制
            self.__log.info("check_limit_num_can_run owner_id: %d, "
                "task_type: %d, type_num: %d, owner_num: %d, db type owner num: %d, test time: %d" % (
                owner_id, task_type, type_num, owner_num,
                self.__owner_task_num_map[task_type][owner_id], self.__test_time))
            if task_type not in self.__type_task_num_map:
                self.__type_task_num_map[task_type] = 0

            if self.__type_task_num_map[task_type] + type_num > \
                    task_util.global_task_limit_map[task_type]:
                self.__log.info("task type[%d] num[%d] "
                        "is exceeded gived num[%d]" % (
                        task_type,
                        self.__type_task_num_map[task_type],
                        task_util.global_task_limit_map[task_type]))
                self.__del_wait_running_limit_num(owner_id, task_type)
                return False

            # 对应用户的对应任务类型数是否超过了限制
            if self.__owner_task_num_map[task_type][owner_id] + owner_num > \
                    task_util.global_each_user_task_limit_map[task_type]:
                self.__del_wait_running_limit_num(owner_id, task_type)
                return False
        except Exception as ex:
            self.__log.error("handle request failed![ex:%s][trace: %s]" % (
                    str(ex), traceback.format_exc()))
            return False
        return True

    def __add_wait_running_limit_num(self, owner_id, task_type):
        owner_id = int(owner_id)
        task_type = int(task_type)

        if task_type in self.__wait_running_type_map:
            self.__wait_running_type_map[task_type] += 1
        else:
            self.__wait_running_type_map[task_type] = 1

        if task_type in self.__wait_running_owner_map:
            if owner_id in self.__wait_running_owner_map[task_type]:
                self.__wait_running_owner_map[task_type][owner_id] += 1
            else:
                self.__wait_running_owner_map[task_type][owner_id] = 1
        else:
            tmp_map = {}
            tmp_map[owner_id] = 1
            self.__wait_running_owner_map[task_type] = tmp_map

    def __del_wait_running_limit_num(self, owner_id, task_type):
        owner_id = int(owner_id)
        task_type = int(task_type)

        if task_type in self.__wait_running_type_map:
            if self.__wait_running_type_map[task_type] > 0:
                self.__wait_running_type_map[task_type] -= 1

        if task_type in self.__wait_running_owner_map:
            if owner_id in self.__wait_running_owner_map[task_type]:
                if self.__wait_running_owner_map[task_type][owner_id] > 0:
                    self.__wait_running_owner_map[task_type][owner_id] -= 1

    def del_wait_running_limit_num(self, owner_id, task_type):
        local_lock = task_util.AutoLock(self.__limit_lock)
        self.__del_wait_running_limit_num(owner_id, task_type)

    def __get_wait_runnint_num(self, owner_id, task_type):
        owner_id = int(owner_id)
        task_type = int(task_type)

        type_num = 0
        owner_num = 0
        if task_type in self.__wait_running_type_map:
            type_num = self.__wait_running_type_map[task_type]

        if task_type in self.__wait_running_owner_map:
            if owner_id in self.__wait_running_owner_map[task_type]:
                owner_num = self.__wait_running_owner_map[task_type][owner_id]
        return type_num, owner_num

    def __get_task_limit_num_from_db(self):
        owner_task_num_map = {}
        type_task_num_map = {}

        type_tasks = self.__sql_manager.get_task_num_with_group_type()
        if type_tasks is not None:
            for task in type_tasks:
                type_task_num_map[int(task[0])] = int(task[1])

        owner_type_tasks = self.__sql_manager.get_owner_task_num_with_type()
        if owner_type_tasks is not None:
            for task in owner_type_tasks:
                task_type = int(task[0])
                owner_id = int(task[1])
                if int(task[0]) not in owner_task_num_map:
                    tmp_map = {}
                    tmp_map[int(task[1])] = int(task[2])
                    owner_task_num_map[int(task[0])] = tmp_map
                else:
                    owner_task_num_map[int(task[0])][int(task[1])] = int(task[2])

                self.__log.info("check_limit_num_can_run owner_id: %d, "
                    "task_type: %d, type_num: %d, owner_num: %d, db type owner num: %d" % (
                    owner_id, task_type, 0, 0, owner_task_num_map[task_type][owner_id]))
        else:
            self.__log.info("failed get owner task num owner num")
            
        local_lock = task_util.AutoLock(self.__limit_lock)
        self.__owner_task_num_map = owner_task_num_map
        self.__type_task_num_map = type_task_num_map
        self.__wait_running_type_map = {}
        return True
    