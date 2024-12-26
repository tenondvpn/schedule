###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import datetime
import threading
import sys
import logging

import crontab

import admin_sql_manager
import check_limit_num
sys.path.append('../common')
import task_util

class AdminTaskBase(threading.Thread):
    """
        admin任务处理基类，安全管理全局任务流程图，检查任务数限制
    """
    def __init__(self):
        threading.Thread.__init__(self)
        self._log = logging
        self.__sql_manager = admin_sql_manager.SqlManager()

    def _get_task_info_copy(self):
        return check_limit_num.CheckLimitNum().get_task_info_copy()

    def _set_task_info_copy(
            self, 
            edge_map,
            task_map, 
            pipeline_map, 
            sub_graph_list, 
            graph):
        check_limit_num.CheckLimitNum().set_task_info_copy(
                edge_map,
                task_map, 
                pipeline_map, 
                sub_graph_list, 
                graph)

    def _check_limit_num_can_run(self, owner_id, task_type):
        return check_limit_num.CheckLimitNum().check_limit_num_can_run(
                owner_id,
                task_type)

    # 获取每一个图的起始节点
    def _get_no_dep_nodes(self, graph_list):
        node_list = []
        for graph in graph_list:
            graph_nodes = graph.nodes()
            for node in graph_nodes:
                if len(list(graph.predecessors(node))) == 0:
                    node_list.append(node)
        return node_list

    def _get_graph_by_node(self, node, sub_graph_list):
        for tmp_graph in sub_graph_list:
            if tmp_graph.has_node(node):
                return tmp_graph
        return None
      
    # 多个依赖规则：
    # 细粒度优先级低于粗粒度，
    # 同粒度下，时间戳越大优先级越高
    def _max_ct_time(self, lhs_item, rhs_item):
        lhs_ct_time = task_util.StaticFunction.strip_with_one_space(lhs_item)
        lhs_ct_time_list = lhs_ct_time.split(" ")
        if len(lhs_ct_time_list) != task_util.CONSTANTS.CT_TIME_SPLIT_NUM:
            return rhs_item
        rhs_ct_time = task_util.StaticFunction.strip_with_one_space(rhs_item)
        rhs_ct_time_list = rhs_ct_time.split(" ")
        if len(rhs_ct_time_list) != task_util.CONSTANTS.CT_TIME_SPLIT_NUM:
            return lhs_item
        
        for i in range(task_util.CONSTANTS.CT_TIME_SPLIT_NUM - 1, -1, -1):
            if lhs_ct_time_list[i] == '*' and rhs_ct_time_list[i] == '*':
                continue

            if lhs_ct_time_list[i] != '*' and rhs_ct_time_list[i] != '*':
                lhs_sub_ct_time_list = lhs_ct_time_list[i].split(',')
                rhs_sub_ct_time_list = rhs_ct_time_list[i].split(',')

                if len(lhs_sub_ct_time_list) > len(rhs_sub_ct_time_list):
                    return rhs_item

                if len(lhs_sub_ct_time_list) < len(rhs_sub_ct_time_list):
                    return lhs_item

                # 第一个最大run_time作为优先级最大的调度者，保证稳定排序
                tmp_len = len(lhs_sub_ct_time_list)
                for j in range(0, tmp_len):
                    try:
                        if int(lhs_sub_ct_time_list[j]) \
                                > int(rhs_sub_ct_time_list[j]):
                            return lhs_item
                        elif int(lhs_sub_ct_time_list[j]) \
                                < int(rhs_sub_ct_time_list[j]):
                            return rhs_item
                    except Exception as ex:
                        self._log.error("catched error: %s" % str(ex))
                        return lhs_item
                continue

            if lhs_ct_time_list[i] != '*':
                return lhs_item
            return rhs_item
        return lhs_item

    def _get_max_prev_ct_time(self, prev_node_list, pipeline_map, task_map):
        if prev_node_list is None:
            return None, None
        max_ct_time = '* * * * *'
        max_task_id = None
        for task_id in prev_node_list:
            tmp_max_ct_time = self._max_ct_time(
                    max_ct_time, 
                    pipeline_map[task_map[task_id][1]].ct_time)
            if tmp_max_ct_time == max_ct_time:
                continue
            max_ct_time = tmp_max_ct_time
            max_task_id = task_id

        if max_ct_time == '* * * * *' or max_task_id is None:
            return None, None

        return max_ct_time, max_task_id

    # 判断上游依赖在时间段内完成的规则
    def _get_min_ct_time(self, ct_time):
        ct_time = task_util.StaticFunction.strip_with_one_space(ct_time)
        ct_time_list = ct_time.split(' ')
        if ct_time_list[1] != '*':
            if ct_time_list[1].find(',') != -1:
                return "%Y%m%d%H00"
            else:
                return "%Y%m%d0000"

        if ct_time_list[0] != '*':
            if ct_time_list[0].find(',') != -1:
                return "%Y%m%d%H%M"
            else:
                return "%Y%m%d%H00"
        return "%Y%m%d0000"

    # 获取上游需要检查成功任务的时间段，比如小时级别依赖分钟级别，
    # 则需要上游在当前调度时间直到上一个调度时间对小时取整时间戳
    # 的时间段内的任务全部执行成功
    # 比如：本任务调度时间是： 201505011001 ct_time 是 1 10 * * *
    #       上游依赖任务ct_time是： 5 * * * *
    #       则上游需要保证：201504300000 ~ 201505011001之间的任务全部成功
    # 其他类推
    def _get_run_time_period_min(self, run_time, ct_time, delta_minutes=-1):
        year = int(run_time[0: 4])
        month = int(run_time[4: 6])
        day = int(run_time[6: 8])
        hour = int(run_time[8: 10])
        minute = int(run_time[10: 12])
        run_datetime = datetime.datetime(year, month, day, hour, minute, 0)
        if delta_minutes < 0:
            run_datetime = run_datetime - datetime.timedelta(
                    minutes=-delta_minutes)
        else:
            run_datetime = run_datetime + datetime.timedelta(
                    minutes=delta_minutes)

        crontab_job = crontab.CronTab(tab='').new(command='/usr/bin/echo')
        crontab_job.setall(ct_time.strip())
        if not crontab_job.is_valid():
            
            return None

        return crontab_job.schedule(run_datetime).get_prev().strftime(
                self._get_min_ct_time(ct_time.strip()))

    def _check_prev_tasks_all_succeeded(
            self, 
            run_time, 
            pipeline_map, 
            task_map, 
            prev_nodes, 
            max_ct_time):
        if prev_nodes is None or len(prev_nodes) <= 0:
            return 0

        max_ct_time = task_util.StaticFunction.strip_with_one_space(
                max_ct_time)
        ct_time_list = max_ct_time.split(' ')
        ct_time_len = len(ct_time_list)
        if ct_time_len != task_util.CONSTANTS.CT_TIME_SPLIT_NUM:
            return 1

        max_ct_time_pos = -1
        for i in range(ct_time_len - 2, -1, -1):
            if ct_time_list[i] != '*':
                max_ct_time_pos = i
                break

        if max_ct_time_pos == -1:
            return 1

        check_equal_level_list = []
        check_slow_level_list = []
        for node in prev_nodes:
            if pipeline_map[task_map[node][1]].ct_time is None \
                    or pipeline_map[task_map[node][1]].ct_time.strip() == '':
                check_equal_level_list.append(node)
                continue

            ct_time = task_util.StaticFunction.strip_with_one_space(
                    pipeline_map[task_map[node][1]].ct_time)
            tmp_ct_time_list = ct_time.split(' ')
            if len(tmp_ct_time_list) != task_util.CONSTANTS.CT_TIME_SPLIT_NUM:
                check_equal_level_list.append(node)
                check_slow_level_list.append(node)
                continue

            if tmp_ct_time_list[max_ct_time_pos] == '*':
                check_slow_level_list.append(node)
            else:
                check_equal_level_list.append(node)

        min_run_time = self._get_run_time_period_min(
                run_time, 
                max_ct_time)
        middle_run_time = min_run_time
        if max_ct_time_pos == 0:
            middle_run_time = "%s00" % run_time[0: 10]
        else:
            middle_run_time = "%s0000" % run_time[0: 8]

        if middle_run_time < min_run_time:
            middle_run_time = min_run_time

        if len(check_slow_level_list) > 0:
            # 检查所有上游依赖是否完成
            if not self.__sql_manager.check_all_task_succeeded(
                    check_slow_level_list, 
                    min_run_time, 
                    run_time):
                return False

        if len(check_equal_level_list) > 0:
            # 检查所有上游依赖是否完成
            if not self.__sql_manager.check_all_task_succeeded(
                    check_equal_level_list, 
                    middle_run_time, 
                    run_time):
                return False
            
        return True
