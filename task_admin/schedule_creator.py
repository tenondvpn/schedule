###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import datetime
import sys
import logging
import traceback
from functools import reduce

import crontab
import horae.models
import django.db

import admin_sql_manager
import admin_task_base
sys.path.append('../common')
import graph_group
import task_util

class ScheduleItem(object):
    def __init__(self, task_id, ct_time, run_time):
        self.task_id = task_id
        self.ct_time = ct_time
        self.run_time = run_time

    def __eq__(self, rhs):
        if self.task_id == rhs.task_id \
                and self.run_time == rhs.run_time \
                and self.ct_time == rhs.ct_time:
            return True
        return False

    def __str__(self):
        return "%d:%s:%s" % (self.task_id, self.ct_time, self.run_time)

class ScheduleCreator(admin_task_base.AdminTaskBase):
    """
        任务生成者
        1.从pipeline，task生成调度任务
        2.产生流程图
        3.保存task，pipeline相关信息，避免频繁操作DB
        4.检查跳跃ct_time，增强容错性
        5.从db更新频率为3分钟，减小db压力，
        6.调度频率为10s，从内存保存的任务信息进行调度，提高用户体验
    """
    def __init__(self):
        admin_task_base.AdminTaskBase.__init__(self)
        self.__graph = graph_group.GraphGroup()
        self.__sub_graph_list = []
        self.__log = logging
        self.__task_map = {}
        self.__pipeline_map = {}
        self.__sql_manager = admin_sql_manager.SqlManager()
        self.__init_time = None
        self.__sql_list = []
        self.__src_list = []
        self.__task_ct_time_map = {}
        self.__begin_node_run_time_map = {}
        self.__schedule_set = set()
        self.__pipe_task_map = {}
        self.__visit_db_time = 0
        self.__CONST_VISIT_DB_PERIOD = 180.0
        self.__init_time = datetime.datetime.utcnow() + \
                datetime.timedelta(hours=8, minutes=1)

    def __init_graph(self):
        self.__task_map = {} 
        self.__pipeline_map = {} 
        self.__sub_graph_list = []
        self.__graph = graph_group.GraphGroup()
        self.__pipe_task_map = {}
        self.__edge_map = {}

    def run(self):
        self.__log.info("ScheduleCreator thread starting...")
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            # admin是否获取了zookeeper锁，具有处理数据的权限
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                time.sleep(1)
                continue

            while True:
                begin_time = time.time()
                self.__log.info("ScheduleCreator handle data starting...")
                # 初始化相关数据
                # 防止now = utcnow
                self.__init_time = datetime.datetime.utcnow() + \
                        datetime.timedelta(hours=8, minutes=1)
                self.insert_values = ""
                self.__sql_list = []
                self.__src_list = []
                self.__task_ct_time_map = {}
                self.__schedule_set.clear()

                self.__create_schedule()
                use_time = time.time() - begin_time
                self.__log.info("ScheduleCreator handle data "
                        "exit.use time[%f]" % use_time)
                break

            time.sleep(10)  # 失败后任然需要sleep

        self.__log.info("ScheduleCreator thread existed!")

    # 为了减小db压力，且提升任务调度实时性，
    # 访问db休眠时间将加长，但是调度休眠时间减小
    def _update_task_info(self, force=False):
        self.__init_graph()
        # 创建临时图
        if not self.__create_graph():
            self.__log.error("schedule creator create graph failed!")
            return False

        # 将pipeline组成独立图组
        self.__create_pipeline_graph_list()               
        # 图组创建好后，类成员变量更新
        self._set_task_info_copy(
                self.__edge_map, 
                self.__task_map, 
                self.__pipeline_map, 
                self.__sub_graph_list,
                self.__graph)
        return True

    def __create_schedule(self):
        if not self._update_task_info():
            return False

        # 将可执行任务放入临时buff中
        if not self.__put_valid_task_into_schedule():
            self.__log.error(
                    "schedule creator create task buffer failed!")
            return False

        # 批量事务写schedule
        if not self.__insert_all_values_into_schedule():
            self.__log.error(
                    "schedule creator write task to db failed!")
            return False
        return True

    def __create_graph(self):
        tried_num = 0
        while True:
            if not self.__sql_manager.get_all_tasks(
                    self.__task_map, 
                    self.__pipeline_map,
                    self.__pipe_task_map):
                self.__log.error("get tasks from db failed!")
                tried_num += 1
                if tried_num > 10:
                    return False
                time.sleep(0.1)
                continue
            break

        for task_key in self.__task_map:
            self.__graph.add_node(task_key)
        
        edges = horae.models.Edge.objects.all()
        for edge in edges:
            self.__graph.add_eage(str(edge.prev_task_id), str(edge.next_task_id))
            if edge.prev_task_id not in self.__edge_map:
                self.__edge_map[edge.prev_task_id] = []

            self.__edge_map[edge.prev_task_id].append(edge)

        return True

    def __create_pipeline_graph_list(self):
        for pl_id in self.__pipe_task_map:
            tmp_graph_group = graph_group.GraphGroup()
            for task in self.__pipe_task_map[pl_id]:
                # 将节点及其下游节点放入图中
                if task[0] in self.__edge_map:
                    for edge in self.__edge_map[task[0]]:
                        tmp_graph_group.add_eage(str(task[0]), str(edge.next_task_id))

                tmp_graph_group.add_node(str(task[0]))

            tmp_graph_group.get_sub_graphs(self.__sub_graph_list)

        return True

    def __get_crontab_iter(self, task_info):
        ct_time = self.__pipeline_map[task_info[1]].ct_time.strip()
        ct_time = task_util.StaticFunction.strip_with_one_space(ct_time)
        if ct_time == '':
            return None

        crontab_job = crontab.CronTab(tab='').new(command='/usr/bin/echo')
        crontab_job.setall(ct_time.strip())
        if not crontab_job.is_valid():
            self.__log.error("job set cron_express[%s] failed!" % ct_time)
            return None
        return crontab_job.schedule(self.__init_time)

    def __get_all_successors_recurrence(self, graph, begin_node, nodes_list):
        if begin_node is None:
            return

        successors = list(graph.successors(begin_node))
        if len(successors) <= 0:
            return

        for suc_node in successors:
            nodes_list.append(suc_node)
            self.__get_all_successors_recurrence(graph, suc_node, nodes_list)

    # 先保存需要插入schedule的字段值，然后采用事务，批量插入
    # 如果不采用事务，且不批量插入，有可能会导致一部分插入成功，一部分失败
    def __keep_all_insert_values(self, begin_node, tmp_run_time):
        nodes_list = []
        nodes_list.append(begin_node)
        self.__get_all_successors_recurrence(
                self.__graph.get_graph(), 
                begin_node, 
                nodes_list)
        for tmp_task_id in nodes_list:
            schedule_item = ScheduleItem(
                    begin_node, 
                    self.__pipeline_map[
                            self.__task_map[begin_node][1]].ct_time, 
                    tmp_run_time)
            if tmp_task_id not in self.__task_ct_time_map:
                tmp_list = []
                tmp_list.append(schedule_item)
                self.__task_ct_time_map[tmp_task_id] = tmp_list
            else:
                self.__task_ct_time_map[tmp_task_id].append(schedule_item)
        return True

    def __check_and_keep_all_value(self, begin_node, task_id, run_time):
        if not self.__sql_manager.task_has_inserted_to_schedule(
                task_id, 
                run_time):
            self.__keep_all_insert_values(
                    begin_node,
                    run_time)
            return True
        return False
    
    def __check_and_insert_to_schedule(self, task_id, run_time):
        if self.__pipeline_map[self.__task_map[task_id][1]].enable != 1:
            return True

        if run_time <= self.__task_map[task_id][9]:
            return False

        if self.__sql_manager.task_has_inserted_to_schedule(
                task_id, 
                run_time):
            return False

        set_item = "%s:%s" % (task_id, run_time)
        if set_item in self.__schedule_set:
            return False

        self.__schedule_set.add("%s:%s" % (task_id, run_time))
        schedule = horae.models.Schedule(
                task_id=task_id,
                run_time=run_time,
                init_time=self.__init_time.strftime("%Y-%m-%d %H:%M:%S"),
                status=task_util.TaskState.TASK_WAITING,
                pl_id=self.__task_map[task_id][1])
        self.__sql_list.append(schedule)

        # 查询历史可以看到等待中的任务
        begin_time = task_util.StaticFunction.get_now_format_time(
                "%Y-%m-%d %H:%M:%S")
        self.__sql_list.append(horae.models.RunHistory(
                task_id=task_id,
                run_time=run_time,
                pl_id=self.__task_map[task_id][1],
                pl_name=self.__pipeline_map[self.__task_map[task_id][1]].name,
                task_name=self.__task_map[task_id][8],
                status=task_util.TaskState.TASK_WAITING,
                # task_id和run_time可以唯一确定一条记录，
                # schedule_id后续更新状态再更新，没有也不影响业务
                schedule_id=0, 
                type=self.__task_map[task_id][6],
                cpu=0,
                mem=0,
                end_time=begin_time,
                start_time=begin_time))
        return True

    def __check_and_update_task_last_run_time(self, task_id, last_run_time):
        if last_run_time <= self.__task_map[task_id][9]:
            return False

        '''
        if self.__sql_manager.task_has_inserted_to_schedule(
                task_id, 
                last_run_time):
            return False
        '''
        try:
            task_obj = horae.models.Task.objects.get(id=task_id)
            if task_obj.last_run_time >= last_run_time:
                return False
            #task_obj.last_run_time = last_run_time
            task_sql = "update horae_task set last_run_time='%s' where id = %s;" % (last_run_time, task_obj.id)
            
            self.__src_list.append(task_sql)
            return True
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return False
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return False

    # 多个依赖规则：
    # 细粒度优先级低于粗粒度，
    # 同粒度下，时间戳越大优先级越高
    def __compare_item(self, lhs_item, rhs_item):
        max_ct_time = self._max_ct_time(lhs_item.ct_time, rhs_item.ct_time)
        if max_ct_time == lhs_item.ct_time:
            return lhs_item
        else:
            return rhs_item

    # 事务批量执行sql，包括更新每个task的最后调度时间以及将task插入schedule
    def __insert_all_values_into_schedule(self):
        for task_id in self.__task_ct_time_map:
            # 没有开启调度的任务不调度
            if self.__pipeline_map[self.__task_map[task_id][1]].enable != 1:
                continue

            max_task = reduce(
                    lambda x, y: self.__compare_item(x, y),
                    self.__task_ct_time_map[task_id])
            if not self.__check_and_update_task_last_run_time(
                    task_id, 
                    max_task.run_time):
                continue

            # 将max_task任务写入db
            if not self.__check_and_insert_to_schedule(
                    task_id, 
                    max_task.run_time):
                continue

            for pre_task in self.__task_ct_time_map[task_id]:
                if pre_task.task_id == max_task.task_id \
                        and pre_task.run_time != max_task.run_time:
                    # 将所有任务写入db
                    self.__check_and_insert_to_schedule(
                            task_id, 
                            pre_task.run_time)
        
        return self.__sql_manager.batch_execute_with_affect_one(
                self.__sql_list, None, self.__src_list)

    # 轮询，直到调度到了task最后一次调度时间，防止任务跳跃
    def __handle_all_ct_time_by_task(self, crontab_iter, task_id):
        run_time = crontab_iter.get_prev().strftime(
                task_util.CONSTANTS.RUN_TIME_FORMAT)
        last_run_time = self.__task_map[task_id][9]
        if last_run_time is None or last_run_time.strip() == '':
            # 如果DB中这个字段为空，则默认用上一个调度时间作为截止点
            last_run_time = crontab_iter.get_prev().strftime(
                    task_util.CONSTANTS.RUN_TIME_FORMAT)

        self.__begin_node_run_time_map[task_id] = run_time
        # 将最近的一个任务直接放入图中
        self.__keep_all_insert_values(
                task_id,
                run_time)
        if run_time <= last_run_time:
            return True

        """
        if self.__sql_manager.task_has_inserted_to_schedule(
                task_id, 
                run_time):
            return True
        """

        run_time = crontab_iter.get_prev().strftime(
                task_util.CONSTANTS.RUN_TIME_FORMAT)
        while not task_util.CONSTANTS.GLOBAL_STOP:
            if run_time <= last_run_time:
                return True

            # 如果schedule中已经有了任务，无需继续检查跳跃ct_time
            if not self.__check_and_keep_all_value(task_id, task_id, run_time):
                return True
            run_time = crontab_iter.get_prev().strftime(
                    task_util.CONSTANTS.RUN_TIME_FORMAT)
        return True

    # 处理每一个图的起始节点，如果ct_time满足调度时间，则写入schedule表
    def __put_valid_task_into_schedule(self):
        no_dep_node_list = self._get_no_dep_nodes(self.__sub_graph_list)
        for task_id in no_dep_node_list:
            if self.__pipeline_map[self.__task_map[task_id][1]].enable != 1:
                continue

            crontab_iter = self.__get_crontab_iter(self.__task_map[task_id])
            if crontab_iter is None:
                # 没有ct time，则不处理
                self.__log.warn("ct time error![pipeline:%s][ct_time:%s]" % (
                    self.__pipeline_map[self.__task_map[task_id][1]].name,
                    self.__pipeline_map[self.__task_map[task_id][1]].ct_time))
                continue
            
            if not self.__handle_all_ct_time_by_task(
                    crontab_iter, 
                    task_id):
                return False
        return True

if __name__ == "__main__":
    schedule_creator = ScheduleCreator()
    schedule_creator.run()
    schedule_creator.join()
