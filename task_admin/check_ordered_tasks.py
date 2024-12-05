###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import sys
import traceback
import logging

import horae.models
import django.core.exceptions

import admin_sql_manager
import admin_task_base
sys.path.append('../common')
import task_util

class CheckOrderedTasks(admin_task_base.AdminTaskBase):
    """
        处理用户提交的需要串行并发的任务
    """
    def __init__(self):
        admin_task_base.AdminTaskBase.__init__(self)
        self.__log = logging
        self.__sql_manager = admin_sql_manager.SqlManager()
        self.__sql_del_list = []
        self.__sql_save_list = []
        self.__sql_list = []

    def run(self):
        self.__log.info("CheckUnexpectStatusTasks thread starting...")
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            # admin是否获取了zookeeper锁，具有处理数据的权限
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                time.sleep(1)
                continue

            while True:
                begin_time = time.time()
                self.__log.info("CheckOrderedTasks handle data started")

                try:
                    self.__sql_del_list = []
                    self.__sql_save_list = []
                    self.__sql_list = []
                    self.__handle_ordered_tasks()
                except django.db.OperationalError as ex:
                    django.db.close_old_connections()
                    self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                            str(ex),
                            traceback.format_exc()))
                except Exception as ex:
                    self.__log.error("RestartTask task failed![ex:%s][trace:%s]" % (
                            str(ex), traceback.format_exc()))

                use_time = time.time() - begin_time
                self.__log.info("CheckOrderedTasks handle data "
                        "exit.use time[%f]" % use_time)
                break

            time.sleep(10)  # 失败后任然需要sleep

        self.__log.info("CheckOrderedTasks thread existed!")
     
    def __set_ordered_tasks_run(self, ordered_id, run_tag):
        user_schedules = self.__sql_manager.get_ordered_schedules(
                ordered_id, 
                run_tag)
        if user_schedules is None:
            return False

        for user_schedule in user_schedules:
            user_schedule.status = task_util.TaskState.TASK_RUNNING
            self.__sql_save_list.append(user_schedule)
            horae_schedules = horae.models.Schedule.objects.filter(
                    run_time=user_schedule.run_time, 
                    task_id=user_schedule.task_id)
            horae_schedule = None
            if len(horae_schedules) <= 0:
                horae_schedule = horae.models.Schedule(
                        task_id=user_schedule.task_id,
                        run_time=user_schedule.run_time,
                        status=task_util.TaskState.TASK_WAITING,
                        pl_id=user_schedule.pl_id,
                        start_time=user_schedule.start_time,
                        init_time=user_schedule.init_time)
            else:
                horae_schedule = horae_schedules[0]
                horae_schedule.start_time = user_schedule.start_time
                horae_schedule.init_time = user_schedule.init_time
                horae_schedule.status = task_util.TaskState.TASK_WAITING
            self.__sql_save_list.append(horae_schedule)

        if not self.__sql_manager.batch_execute_with_affect_one(
                self.__sql_save_list, None, None):
            self.__log.error("batch_execute_with_affect_one failed!")
            return False
        return True

    # 获取用户提供需要串行并发的任务
    def __handle_ordered_tasks(self):
        ordered_id_list = self.__sql_manager.get_all_user_schedule_order_id()
        if ordered_id_list is None:
            return True

        if len(ordered_id_list) <= 0:
            return True

        for ordered_id in ordered_id_list:
            user_schedule = self.__sql_manager.\
                get_min_run_time_user_user_schedule_task(ordered_id[0])

            if user_schedule is None:
                continue

            if user_schedule.status == task_util.TaskState.TASK_RUNNING:
                continue

            if user_schedule.status == task_util.TaskState.TASK_WAITING:
                self.__set_ordered_tasks_run(
                        user_schedule.ordered_id, 
                        user_schedule.run_tag)
        return True
