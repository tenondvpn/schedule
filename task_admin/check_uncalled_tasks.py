###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import sys
import copy
import logging

import horae.models

import admin_task_base
import admin_sql_manager
sys.path.append('../common')
import task_util

class CheckUncalledTasks(admin_task_base.AdminTaskBase):
    """
        从schedule表中获取waiting状态的任务，检查其是否不能被自动调度
    """
    def __init__(self, task_node_manager):
        admin_task_base.AdminTaskBase.__init__(self)
        self.__log = logging
        self.__sql_manager = admin_sql_manager.SqlManager()
        self.__sub_graph_list = []
        self.__edge_map = {}
        self.__task_map = {}
        self.__pipeline_map = {}
        self.__sql_del_list = []
        self.__sql_save_list = []
        self.__sql_list = []
        self.__task_node_manager = task_node_manager
        self.__graph = None

    def run(self):
        self.__log.info("CheckUncalledTasks thread starting...")
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            # admin是否获取了zookeeper锁，具有处理数据的权限
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                time.sleep(1)
                continue
            while True:
                begin_time = time.time()
                self.__log.info("CheckUncalledTasks handle data starting...")
                del self.__edge_map
                del self.__task_map
                del self.__pipeline_map
                del self.__sub_graph_list
                del self.__graph

                self.__edge_map, \
                self.__task_map, \
                self.__pipeline_map, \
                self.__sub_graph_list, \
                self.__graph = self._get_task_info_copy()
                if len(self.__sub_graph_list) <= 0:
                    break

                self.__call_waiting_tasks()

                use_time = time.time() - begin_time
                self.__log.info("CheckUncalledTasks handle data "
                        "exit.use time[%f]" % use_time)
                break

            time.sleep(10)  # 失败后任然需要sleep

        self.__log.info("CheckUncalledTasks thread existed!")

    def __update_task_info_to_stop_by_dispatch_tag(self, schedule):
        now_time = task_util.StaticFunction.get_now_format_time(
                "%Y-%m-%d %H:%M:%S")
        # 更新schedule
        schedule_sql = ("update horae_schedule set status = %d, "
                "start_time = '%s' where id = %d and status = %d;" % (
                task_util.TaskState.TASK_PREV_FAILED, 
                now_time, 
                schedule.id, 
                task_util.TaskState.TASK_WAITING))
        self.__sql_list.append(schedule_sql)
        # 写run_history
        run_history_sql = ("update horae_runhistory set status = %d, "
                "start_time = '%s', end_time = '%s', "
                "run_server = '%s', schedule_id = %d "
                "where task_id = %s "
                "and status = %d and run_time = '%s';" % (
                task_util.TaskState.TASK_PREV_FAILED, 
                now_time, 
                now_time, 
                "",
                schedule.id,
                schedule.task_id,
                task_util.TaskState.TASK_WAITING,
                schedule.run_time))
        self.__sql_list.append(run_history_sql)

    def __write_new_task_into_ready_task(self, task_id, run_time, schedule):
        now_time = task_util.StaticFunction.get_now_format_time(
                "%Y-%m-%d %H:%M:%S")
        tmp_run_server = self.__task_node_manager.get_valid_ip(
                self.__task_map[task_id][6], 
                self.__task_map[task_id][5])
        if tmp_run_server is None:
            self.__log.error("get task node run_server failed![task_id:%s]"
                    "[type:%d][server_tag:%s]" % (
                    task_id, 
                    self.__task_map[task_id][6], 
                    self.__task_map[task_id][5]))
            return False

        # 写入ready_task
        ready_task = horae.models.ReadyTask(
                pl_id=self.__task_map[task_id][1],
                schedule_id=schedule.id,
                status=task_util.TaskState.TASK_READY,
                update_time=now_time,
                run_server=tmp_run_server,
                type=self.__task_map[task_id][6],
                init_time=now_time,
                run_time=run_time,
                task_id=task_id,
                task_handler='',
                retry_count=int(self.__task_map[task_id][7]),
                retried_count=0,
                server_tag=self.__task_map[task_id][5],
                is_trigger=0,
                next_task_ids=self.__task_map[task_id][2],
                prev_task_ids=self.__task_map[task_id][3],
                owner_id=self.__pipeline_map[
                        self.__task_map[task_id][1]].owner_id,
                pid=self.__task_map[task_id][4])
        self.__sql_save_list.append(ready_task)

        # 更新schedule
        schedule_sql = ("update horae_schedule set status = %d, "
                "start_time = '%s' where id = %d and status = %d;" % (
                task_util.TaskState.TASK_READY, 
                now_time, 
                schedule.id, 
                task_util.TaskState.TASK_WAITING))
        self.__sql_list.append(schedule_sql)
        # 写run_history
        run_history_sql = ("update horae_runhistory set status = %d, "
                "start_time = '%s', end_time = '%s', "
                "run_server = '%s', schedule_id = %d "
                "where task_id = %s "
                "and status = %d and run_time = '%s';" % (
                task_util.TaskState.TASK_READY, 
                now_time, 
                now_time, 
                tmp_run_server,
                schedule.id,
                schedule.task_id,
                task_util.TaskState.TASK_WAITING,
                schedule.run_time))
        self.__sql_list.append(run_history_sql)

        return True

    def __check_task_uncalled(self, str_task_id, run_time, wait_task_set):
        graph = self._get_graph_by_node(str_task_id, self.__sub_graph_list)
        if graph is None:
            return False
#        pipeline_pre = graph.predecessors(str_task_id)
        # 如果其是pipeline的起始节点，则由task_dispacher调度
#        if len(pipeline_pre) <= 0:
#            return False

        # 检查ready_task中是否有优先级最大的上游节点，
        # 如果没有则认为其没有触发者
        prev_nodes = list(self.__graph.get_graph().predecessors(str_task_id))
        for prev_node in prev_nodes:
            check_item = '%s_%s' % (prev_node, run_time)
            if check_item in wait_task_set:
                return False

        tmp_prev_nodes = copy.deepcopy(prev_nodes)
        tmp_prev_nodes.append(str_task_id)
        prev_max_ct_time, prev_max_task_id = self._get_max_prev_ct_time(
                tmp_prev_nodes, 
                self.__pipeline_map, 
                self.__task_map)

        if prev_max_ct_time is None:
            if self.__sql_manager.check_all_task_succeeded(
                    prev_nodes, 
                    run_time, 
                    run_time):
                return True

            return False

        # 检查所有上游依赖是否完成
        return self._check_prev_tasks_all_succeeded(
                str_task_id,
                run_time, 
                self.__pipeline_map, 
                self.__task_map, 
                self.__edge_map,
                prev_nodes, 
                prev_max_ct_time)

    # 遍历schedule表，获取所有等待中的任务，
    # 检查其是不是不能自动调度的，如果是则启动
    def __call_waiting_tasks(self):
        wait_task_set = set()
        waiting_task_list = self.__sql_manager.get_all_waiting_tasks(
                wait_task_set)
        if waiting_task_list is None or len(waiting_task_list) <= 0:
            return True

        self.__sql_del_list = []
        self.__sql_save_list = []
        self.__sql_list = []
        for waiting_task in waiting_task_list:
            if str(waiting_task.task_id) not in self.__task_map:
                continue

            if not self.__check_task_uncalled(
                    str(waiting_task.task_id), 
                    waiting_task.run_time,
                    wait_task_set):
                continue

            # 检查任务数限制
            owner_id = self.__pipeline_map[waiting_task.pl_id].owner_id
            task_type = self.__task_map[str(waiting_task.task_id)][6]
            if not self._check_limit_num_can_run(owner_id, task_type):
                continue

            if not self.__write_new_task_into_ready_task(
                    str(waiting_task.task_id), 
                    waiting_task.run_time, 
                    waiting_task):
                continue

            if not self.__sql_manager.batch_execute_with_affect_one(
                    self.__sql_save_list, 
                    self.__sql_del_list,
                    self.__sql_list):
                self.__log.warn("save or del db data failed!")
            self.__sql_del_list = []
            self.__sql_save_list = []
            self.__sql_list = []

        if not self.__sql_manager.batch_execute_with_affect_one(
                self.__sql_save_list, 
                self.__sql_del_list,
                self.__sql_list):
            self.__log.warn("save or del db data failed!")
        return True

if __name__ == "__main__":
    print("please run test in db_manager!")
