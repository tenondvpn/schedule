###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import sys
import logging
import traceback

import horae.models

import admin_task_base
import admin_sql_manager
sys.path.append('../common')
import task_util

class CheckSucceededTasks(admin_task_base.AdminTaskBase):
    """
        从ready_task获取状态为TASK_SUCCEED的任务，启动下游依赖任务

        如果完成节点的下游的其他依赖节点有失败的，不改变其状态，
        等待上游失败任务重跑
    """
    def __init__(self, task_node_manager):
        admin_task_base.AdminTaskBase.__init__(self)
        self.__succ_task_list = []
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
        self.__log.info("CheckSuccededTasks thread starting...")
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            # admin是否获取了zookeeper锁，具有处理数据的权限
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                time.sleep(1)
                continue

            begin_time = time.time()
            self.__log.info("CheckSuccededTasks handle data starting...")
            while True:
                self.__edge_map, \
                self.__task_map, \
                self.__pipeline_map, \
                self.__sub_graph_list, \
                self.__graph = self._get_task_info_copy()
                if len(self.__sub_graph_list) <= 0:
                    break

                self.__call_succeeded_tasks()
                break
            use_time = time.time() - begin_time
            self.__log.info("CheckSuccededTasks handle data "
                    "exit.use time[%f]" % use_time)
            time.sleep(10)  # 失败后任然需要sleep

        self.__log.info("CheckSuccededTasks thread existed!")

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

    # 判断所有上游依赖任务是否完成，如果完成启动任务，并改写相关db状态
    def __check_task_can_call(
            self, 
            task_id, 
            run_time, 
            parent_task_id):
        if self.__sql_manager.has_schedule_with_id_run_time(
                task_id, 
                run_time) is None:
            return True

        prev_nodes = list(self.__graph.get_graph().predecessors(task_id))
        if prev_nodes is None:
            self.__log.error("task id[%s] must has prev node[%s]" % (
                    task_id, parent_task_id))
            return False

        prev_max_ct_time, prev_max_task_id = self._get_max_prev_ct_time(
                prev_nodes, 
                self.__pipeline_map, 
                self.__task_map)

        if prev_max_ct_time is None:
            if not self.__sql_manager.check_all_task_succeeded(
                    prev_nodes, 
                    run_time, 
                    run_time):
                return False

        if prev_max_ct_time is not None and prev_max_task_id is not None:
            # 其调度必须由上游优先级最大的节点触发，不是优先级最大的直接删除
            if prev_max_task_id != parent_task_id:
                return True

            # 由其触发的，则不再检查其历史状态
            nodes_len = len(prev_nodes)
            for i in range(0, nodes_len):
                if prev_nodes[i] == prev_max_task_id:
                    del prev_nodes[i]
                    break

            # 检查所有上游依赖是否完成
            if not self._check_prev_tasks_all_succeeded(
                    run_time, 
                    self.__pipeline_map, 
                    self.__task_map, 
                    prev_nodes, 
                    prev_max_ct_time):
                return False

        # 检查任务数限制
        pipeline_id = self.__task_map[str(task_id)][1]
        owner_id = self.__pipeline_map[pipeline_id].owner_id
        task_type = self.__task_map[task_id][6]
        if not self._check_limit_num_can_run(owner_id, task_type):
            return False

        schedule = self.__sql_manager.task_is_in_schedule_with_status(
                task_id, 
                run_time, 
                task_util.TaskState.TASK_WAITING)
        if schedule is None:
            return True

        if not self.__write_new_task_into_ready_task(
                task_id, 
                run_time, 
                schedule):
            self.__log.error("__write_new_task_into_ready_task failed!")
            return False
        return True

    # 遍历ready_task表，获取所有成功的任务，调度其依赖任务
    def __call_succeeded_tasks(self):
        self.__succ_task_list = self.__sql_manager.get_all_succeeded_tasks()
        if self.__succ_task_list is None or len(self.__succ_task_list) <= 0:
            return True

        for succ_task in self.__succ_task_list:
            self.__sql_del_list = []
            self.__sql_save_list = []
            self.__sql_list = []
            user_del_list = []
		
            while True:
                self.__sql_del_list.append(succ_task)
                user_schedule = self.__sql_manager.get_ordered_schedule(
                        succ_task.task_id, 
                        succ_task.run_time)
                if user_schedule is not None:
                    user_del_list.append(user_schedule)

                successors = None
                try:
                    successors = list(self.__graph.get_graph().successors(
                            str(succ_task.task_id)))
                except:
                    self.__log.error(f"CheckSuccededTasks catch error: {traceback.format_exc()}")
                    successors = None

                if successors is None:
                    break

                for next_node in successors:
                    if next_node not in self.__task_map:
                        self.__sql_del_list = []
                        continue

                    if not self.__check_task_can_call(
                            next_node, 
                            succ_task.run_time,
                            str(succ_task.task_id)):
                        # 如果其下游节点不能启动，
                        # 则其本身不能从ready_task中删除，以等待下次调用
                        self.__sql_del_list = []
                break

            for item in user_del_list:
                self.__sql_del_list.append(item)

            if not self.__sql_manager.batch_execute_with_affect_one(
                    self.__sql_save_list, 
                    self.__sql_del_list,
                    self.__sql_list):
                self.__log.warn("save or del db data failed!")


        return True

if __name__ == "__main__":
    print("please run test in db_manager!")
