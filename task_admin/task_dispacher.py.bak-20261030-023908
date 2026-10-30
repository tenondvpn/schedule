###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import time
import datetime
import logging

import horae.models

import admin_task_base
sys.path.append('../common')
import task_util
import admin_sql_manager

class TaskDispatcher(admin_task_base.AdminTaskBase):
    """
        任务分发
        从可执行图中获取每个图的起始任务节点，分派任务到对应的task_node
    """
    def __init__(self, task_node_manager):
        admin_task_base.AdminTaskBase.__init__(self)
        self.__sub_graph_list = []
        self.__log = logging
        self.__sql_manager = admin_sql_manager.SqlManager()
        self.__edge_map = {}
        self.__task_map = {}
        self.__pipeline_map = {}
        self.__owner_task_num_map = {}
        self.__type_task_num_map = {}
        self.__task_node_manager = task_node_manager
        self.__init_time = datetime.datetime.utcnow() + \
                datetime.timedelta(hours=8)
    
    def __init(self):
        self.__init_time = datetime.datetime.utcnow() + \
                datetime.timedelta(hours=8)
        self.__edge_map, \
        self.__task_map, \
        self.__pipeline_map, \
        self.__sub_graph_list, \
        self.__graph = self._get_task_info_copy()
        if len(self.__sub_graph_list) <= 0:
            return False
 
        return True       

    def run(self):
        self.__log.info("TaskDispatcher thread started!")
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            # admin是否获取了zookeeper锁，具有处理数据的权限
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                time.sleep(1)
                continue
            task_util.CONSTANTS.TASK_DISPATCHER_IS_RUNNING = True
            self.__log.info("TaskDispatcher dispatcher started.")
            begin_time = time.time()
            while True:
                if not self.__init():
                    self.__log.warn("TaskDispatcher handle init failed")
                    break
 
                if not self.__get_beign_task_from_schedule():
                    break
                break
            task_util.CONSTANTS.TASK_DISPATCHER_IS_RUNNING = False
            use_time = time.time() - begin_time
            self.__log.info("TaskDispatcher dispatcher "
                    "ended.use time[%f]" % use_time)
            time.sleep(10)
        self.__log.info("TaskDispatcher thread existed!")
  
    def __check_prev_tasks_all_succeeded(self, task_id, run_time):
        prev_nodes = list(self.__graph.get_graph().predecessors(task_id))
        if prev_nodes is None or len(prev_nodes) <= 0:
            return True
        prev_nodes_tmp = prev_nodes[:]
        # 将节点自己的ct_time和所有上游的ct_time获取优先级最大的
        prev_nodes.append(task_id)
        prev_max_ct_time, prev_max_task_id = self._get_max_prev_ct_time(
                prev_nodes, 
                self.__pipeline_map, 
                self.__task_map)

        # 如果其由上游触发，则不启动
        if prev_max_task_id != task_id:
            return False

        # 检查所有上游依赖是否完成
        if not self._check_prev_tasks_all_succeeded(
                run_time, 
                self.__pipeline_map, 
                self.__task_map, 
                prev_nodes_tmp, 
                prev_max_ct_time):
            return False
        return True

    def __new_task_update_db(self, schedule_info):
        if str(schedule_info.task_id) not in self.__task_map:
            self.__log.error("task map has no this task_id:%d,map:%s" % (
                    schedule_info.task_id, str(self.__task_map)))
            return False

        if not self.__check_prev_tasks_all_succeeded(
                str(schedule_info.task_id), 
                schedule_info.run_time):
            return True

        str_task_id = str(schedule_info.task_id)
        pipeline_id = self.__task_map[str_task_id][1]
        owner_id = self.__pipeline_map[pipeline_id].owner_id
        task_type = self.__task_map[str_task_id][6]
        if not self._check_limit_num_can_run(owner_id, task_type):
            return False

        # 获取可以执行的task_node，合法性检查会通过get_valid_ip进行
        tmp_run_server = self.__task_node_manager.get_valid_ip(
                task_type, 
                self.__task_map[str_task_id][5])
        if tmp_run_server is None:
            self.__log.error("get task node run_server failed![task_id:%s]"
                    "[type:%d][server_tag:%s]" % (
                    str_task_id, 
                    task_type, 
                    self.__task_map[str_task_id][5]))
            return False

        sql_save_list = []
        sql_list = []

        start_time = self.__init_time.strftime("%Y-%m-%d %H:%M:%S")
        # 写入ready_task
        sql_save_list.append(horae.models.ReadyTask(
                pl_id=self.__task_map[str_task_id][1],
                schedule_id=schedule_info.id,
                status=task_util.TaskState.TASK_READY,
                update_time=start_time,
                run_server=tmp_run_server,
                type=task_type,
                init_time=start_time,
                run_time=schedule_info.run_time,
                task_id=str_task_id,
                task_handler='',
                retry_count=int(self.__task_map[str_task_id][7]),
                retried_count=0,
                server_tag=self.__task_map[str_task_id][5],
                next_task_ids=self.__task_map[str_task_id][2],
                prev_task_ids=self.__task_map[str_task_id][3],
                is_trigger=0,
                owner_id=self.__pipeline_map[
                        self.__task_map[str_task_id][1]].owner_id,
                pid=self.__task_map[str_task_id][4]))

        # 更新schedule
        schedule_sql = ("update horae_schedule set status = %d, "
                "start_time = '%s' where id = %d and status = %d;" % (
                task_util.TaskState.TASK_READY, 
                start_time, 
                schedule_info.id, 
                task_util.TaskState.TASK_WAITING))
        sql_list.append(schedule_sql)

        # 写run_history
        run_history_sql = ("update horae_runhistory set status = %d, "
                "start_time = '%s', end_time = '%s', "
                "run_server = '%s', schedule_id = %d "
                " where task_id = %s "
                "and status = %d and run_time = '%s';" % (
                task_util.TaskState.TASK_READY, 
                start_time, 
                start_time, 
                tmp_run_server,
                schedule_info.id,
                schedule_info.task_id,
                task_util.TaskState.TASK_WAITING,
                schedule_info.run_time))
        sql_list.append(run_history_sql)

        # 此处写db失败会线程全部重试，不会影响业务
        return self.__sql_manager.batch_execute_with_affect_one(
                sql_save_list, 
                None, 
                sql_list)

    def __get_beign_task_from_schedule(self):
        node_list = self._get_no_dep_nodes(self.__sub_graph_list)
        if len(node_list) <= 0:
            return True

        getted_tasks = self.__sql_manager.\
                get_all_begin_node_task_from_schedule(node_list)
        if getted_tasks is None:
            return True
        # 遍历每一个起始task
        for schedule_info in getted_tasks:
            # 将相关数据写入db
            self.__new_task_update_db(schedule_info)
        return True
