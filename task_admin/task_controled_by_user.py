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
import traceback
import logging
import uuid
import hashlib

import horae.models
import django.core.exceptions
from asgiref.sync import sync_to_async

import schedule_creator
import admin_sql_manager
sys.path.append('../common')
import task_util

class TaskControledByUser(schedule_creator.ScheduleCreator):
    """
        1. 用户选择重跑任务，手动启动多个任务（包括一个），
        如果手动启动的任务中有在运行的，则全部启动失败，
        需要用户先停止相关任务，再提交

        2. 线程不安全
    """
    def __init__(self):
        schedule_creator.ScheduleCreator.__init__(self)
        self.__log = logging
        self.__sql_manager = admin_sql_manager.SqlManager()
        self.__sub_graph_list = []
        self.__edge_map = {}
        self.__task_map = {}
        self.__pipeline_map = {}
        self.__sql_del_list = []
        self.__sql_save_list = []
        self.__sql_src_list = []
        self.__task_name_list = []
        self.__readytask_set = set()
        self.__graph = None
        self.__reload_lock = threading.Lock()

    def run(self):
        pass

    def restart_tasks(self, task_pair_list, ordered):
        ret, error_log, task_run_map = self.__check_task_is_refreshed(
                task_pair_list)
        if ret == 2:
            # 如果缓存数据发现任务不存在，则强制更新缓存
            # task_util.AutoLock(self.__reload_lock)
            self.__reload_lock.acquire()
            self._update_task_info(True)
            ret, error_log, task_run_map = self.__check_task_is_refreshed(task_pair_list)
            self.__reload_lock.release()
        if ret != 0:
            return error_log

        return self.__restart_tasks(task_pair_list, task_run_map, ordered)

    def __check_task_is_refreshed(self, task_pair_list):
        task_run_map = {}
        self.__edge_map, \
        self.__task_map, \
        self.__pipeline_map, \
        self.__sub_graph_list, \
        self.__graph = self._get_task_info_copy()
        if len(self.__sub_graph_list) <= 0:
            error_log = "schedule creator has not called!"
            self.__log.error(error_log)
            print(1)
            return 2, error_log, task_run_map
        now_time = task_util.StaticFunction.get_now_format_time("%Y%m%d%H%M")

        for task_pair in task_pair_list:
            if task_pair[0].strip() not in self.__task_map:
                error_log = ("task id[%s] error, not exist!" % 
                        task_pair[0].strip())
                self.__log.warn(error_log)
                print(2)
                return 2, error_log, task_run_map
            pl_id = None
            task_id = None
            try:
                print("o 1")
                task = horae.models.Task.objects.get(id=int(task_pair[0].strip()))
                if task is None:
                    print(3)
                    return 2, "0 get task from db failed[%s]!" % task_pair[0], task_run_map
                
                edges = horae.models.Edge.objects.filter(prev_task_id=task.id)
                print("o 2")
                pl_id = task.pl_id
                task_id = task.id
                print("o 3")
                for edge in edges:
                    if not self.__graph.get_graph().has_edge(
                            str(task_id), 
                            str(edge.next_task_id)):
                        print(4)
                        return 2, "1 get task from db failed[%s]!" % task_pair[0], task_run_map
                print("o 4")
            except django.db.OperationalError as ex:
                django.db.close_old_connections()
                self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                        str(ex),
                        traceback.format_exc()))
                print(5)
                return 2, "2 get task from db failed[%s][%s]!" % (task_pair[0], str(ex)), task_run_map
            except Exception as ex:
                self.__log.error(str(ex) + \
                        " execute sql failed![get_last_task_run_time]!")
                print(6)
                return 2, "3 get task from db failed[%s][%s]!" % (task_pair[0], str(ex)), task_run_map

            run_time = task_pair[1].strip()
            if len(run_time) != task_util.CONSTANTS.RUN_TIME_LENTGH:
                error_log = "run_time[%s] error length must be[%d]" % (
                        run_time, 
                        task_util.CONSTANTS.RUN_TIME_LENTGH)
                self.__log.warn(error_log)
                print(7)
                return 1, error_log, task_run_map

            if run_time > now_time:
                error_log = (
                        "执行时间[%s] 不能超过当前时间[%s]."
                        "（执行时间会根据调度时间生成）" % (
                        run_time, 
                        now_time))
                self.__log.warn(error_log)
                print(8)
                return 1, error_log, task_run_map

            if pl_id in task_run_map:
                task_run_map[pl_id].append((task_id, run_time))
            else:
                tmp_list = []
                tmp_list.append((task_id, run_time))
                task_run_map[pl_id] = tmp_list

        for pl_id in task_run_map:
            task_run_map[pl_id].sort(key=lambda x: x[1])

        print(9)
        return 0, "OK", task_run_map

    def __restart_tasks(self, task_pair_list, task_run_map, ordered):
        self.__sql_del_list = []
        self.__sql_save_list = []
        self.__sql_src_list = []
        self.__readytask_set.clear()

        ret, error_log = self.__insert_all_task_to_waiting(
                task_pair_list, 
                task_run_map, 
                ordered)
        if not ret:
            return error_log

        # 防止依赖关系没有被task_dipatcher 更新，导致任务被非法启动
        while task_util.CONSTANTS.TASK_DISPATCHER_IS_RUNNING \
                and not task_util.CONSTANTS.GLOBAL_STOP:
            time.sleep(0.01)

        if not self.__sql_manager.batch_execute_with_affect_one(
                self.__sql_save_list, 
                self.__sql_del_list, 
                self.__sql_src_list):
            error_log = "db insert tasks failed!"
            self.__log.error(error_log)
            return error_log
        return "OK"

    def __get_prev_max_task_id(self, task_id):
        prev_nodes = list(self.__graph.get_graph().predecessors(task_id))
        if prev_nodes is None or len(prev_nodes) <= 0:
            return True, None

        prev_max_ct_time, prev_max_task_id = self._get_max_prev_ct_time(
                prev_nodes, 
                self.__pipeline_map, 
                self.__task_map)
        if prev_max_task_id is None:
            self.__log.error("get prev max task id failed!"
                    "%s" % str(prev_nodes))
            return False, None
        return True, prev_max_task_id

    def __get_ordered_id(self):
        sign = str(uuid.uuid1()).encode("utf-8")
        md5 = hashlib.md5()
        md5.update(sign)
        return md5.hexdigest()   

    def __insert_all_task_to_waiting(self, task_pair_list, task_run_map, ordered):
        now_time = task_util.StaticFunction.get_now_format_time(
                "%Y-%m-%d %H:%M:%S")
        for pl_id in task_run_map:
            run_tag = 0
            ordered_index = 0
            ordered_id = self.__get_ordered_id()
            old_run_time = task_run_map[pl_id][0][1].strip()
            for task_pair in task_run_map[pl_id]:
                task_id = str(task_pair[0])
                run_time = task_pair[1].strip()

                if old_run_time != run_time:
                    old_run_time = run_time
                    ordered_index += 1
                    if ordered_index >= ordered:
                        ordered_index = 0
                        run_tag += 1

                try:
                    schedule = horae.models.Schedule.objects.get(
                            task_id=int(task_id), 
                            run_time=run_time)
                except django.db.OperationalError as ex:
                    django.db.close_old_connections()
                    self.__log.error("error:%s" % traceback.format_exc())
                    return False, "db error!"
                except Exception as ex:
                    schedule = None

                try:
                    ready_task = horae.models.ReadyTask.objects.get(
                            task_id=int(task_id), 
                            run_time=run_time)
                    self.__sql_del_list.append(ready_task)
                except Exception as ex:
                    self.__log.error("error:%s" % str(ex))

                try:
                    user_schedule = horae.models.OrderdSchedule.objects.get(
                            task_id=int(task_id), 
                            run_time=run_time)
                except django.db.OperationalError as ex:
                    django.db.close_old_connections()
                    self.__log.error("error:%s" % traceback.format_exc())
                    return False, "db error!"
                except Exception as ex:
                    user_schedule = None

                if ordered > 0:
                    if user_schedule is not None:
                        user_schedule.status = task_util.TaskState.TASK_WAITING
                        user_schedule.ordered_id = ordered_id
                        user_schedule.run_tag = run_tag
                        user_schedule.init_time = now_time
                        self.__sql_save_list.append(user_schedule)
                    else:
                        self.__sql_save_list.append(horae.models.OrderdSchedule(
                                task_id=int(task_id),
                                run_time=run_time,
                                init_time=now_time,
                                status=task_util.TaskState.TASK_WAITING,
                                pl_id=self.__task_map[task_id][1],
                                ordered_id=ordered_id,
                                run_tag=run_tag))

                if schedule is None:
                    if ordered <= 0:
                        if user_schedule is not None:
                            self.__sql_del_list.append(user_schedule)

                        self.__sql_save_list.append(horae.models.Schedule(
                                task_id=int(task_id),
                                run_time=run_time,
                                init_time=now_time,
                                status=task_util.TaskState.TASK_WAITING,
                                pl_id=self.__task_map[task_id][1]))
                    else:
                        self.__sql_save_list.append(horae.models.Schedule(
                                task_id=int(task_id),
                                run_time=run_time,
                                init_time=now_time,
                                status=task_util.TaskState.TASK_STOPED_BY_USER,
                                pl_id=self.__task_map[task_id][1]))

                else:
                    if schedule.status in(
                            task_util.TaskState.TASK_RUNNING,
                            task_util.TaskState.TASK_READY):
                        task = horae.models.Task.objects.get(id=schedule.task_id)
                        error_log = ("task[%s] status [%s] is error!" % (
                                task.name, 
                                task_util.global_status_info_map[schedule.status]))
                        self.__log.warn(error_log)
                
                    # 全部状态都更改，注意后续影响
                    # 更新schedule, 如果是批量重跑需要串行并行运算，则不改shedule
                    if ordered <= 0:
                        if user_schedule is not None:
                            self.__sql_del_list.append(user_schedule)

                        schedule_sql = ("update horae_schedule set status = %d, "
                                "start_time = '%s' where id = %d" % (
                                task_util.TaskState.TASK_WAITING, 
                                now_time, 
                                schedule.id))
                        self.__sql_src_list.append(schedule_sql)
                    else:
                        schedule_sql = ("update horae_schedule set status = %d, "
                                "start_time = '%s' where id = %d" % (
                                task_util.TaskState.TASK_STOPED_BY_USER, 
                                now_time, 
                                schedule.id))
                        self.__sql_src_list.append(schedule_sql)

                try:
                    run_history = horae.models.RunHistory.objects.get(
                            task_id=int(task_id), 
                            run_time=run_time)
                except django.db.OperationalError as ex:
                    django.db.close_old_connections()
                    self.__log.error("error:%s" % traceback.format_exc())
                    return False, "db error!"
                except Exception as ex:
                    run_history = None

                if run_history is None:
                    # 查询历史可以看到等待中的任务
                    begin_time = task_util.StaticFunction.get_now_format_time(
                            "%Y-%m-%d %H:%M:%S")
                    self.__sql_save_list.append(horae.models.RunHistory(
                            task_id=int(task_id),
                            run_time=run_time,
                            pl_id=self.__task_map[task_id][1],
                            pl_name=self.__pipeline_map[
                                    self.__task_map[task_id][1]].name,
                            task_name=self.__task_map[task_id][8],

                            status=task_util.TaskState.TASK_WAITING,
                            # task_id和run_time可以唯一确定一条记录，
                            # schedule_id后续更新状态再更新，没有也不影响业务
                            schedule_id=0,  
                            type=self.__task_map[task_id][6],
                            start_time=begin_time,
                            end_time=begin_time))
                else:
                    # 写重试历史表
                    rerun_history = horae.models.RerunHistory(
                        task_id=run_history.task_id,
                        run_time=run_history.run_time,
                        pl_id=run_history.pl_id,
                        start_time=run_history.start_time,
                        end_time=run_history.end_time,
                        status=run_history.status,
                        schedule_id=run_history.schedule_id,
                        tag=run_history.tag,
                        type=run_history.type,
                        task_handler=run_history.task_handler,
                        run_server=run_history.run_server,
                        server_tag=run_history.server_tag,
                        pl_name=run_history.pl_name,
                        task_name=run_history.task_name)
                    self.__sql_save_list.append(rerun_history)

                    run_history.status = task_util.TaskState.TASK_WAITING
                    run_history.start_time = now_time
                    run_history.end_time = now_time
                    self.__sql_save_list.append(run_history)

        return True, ''

if __name__ == "__main__":
    print("please run test in db_manager!")
