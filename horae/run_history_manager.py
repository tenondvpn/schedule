###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import traceback
import json
import uuid
import urllib.request
import copy
import time
import datetime

import horae
import crontab

from horae import tools_sql_manager
from horae import tools_util
from horae import graph_manager

class RunHistoryManager(object):
    """
        流程管理接口实现
    """
    def __init__(self, logger, pipeline_manager=None):
        self.__log = logger
        self.__sql_manager = tools_sql_manager.SqlManager(logger)
        self.__pipeline_mgr = pipeline_manager
        self.__graph_mgr = graph_manager.GraphGroup(logger)

    # 执行状态管理
    def show_run_history(
            self,
            show_tag,
            owner_id,
            page_min,
            page_max,
            order_field,
            sort_order,
            search_list):
        try:
            where_content = ''
            if search_list is not None and len(search_list) > 0:
                tmp_content = self.__create_where_content(search_list)
                if tmp_content.strip() != '':
                    where_content = "and %s" % tmp_content

            return self.__show_task_history(
                    owner_id,
                    page_min,
                    page_max,
                    order_field,
                    sort_order,
                    where_content)
        except Exception as ex:
            self.__log.error("error ex: %s, trace: %s" % (
                    str(ex), traceback.format_exc()))
            return self.__get_default_ret_map(1, str(ex))

    def __create_where_content(self, search_list):
        if search_list is None:
            return ""

        content_list = []
        for item in search_list:
            key = item[0]
            value = item[1]
            if value == '':
                continue

            if str(value).startswith('=') \
                    or str(value).startswith('>') \
                    or str(value).startswith('<') \
                    or str(value).startswith('!'):
                content_list.append("%s %s" % (key, value))
                continue
            content_list.append("%s like '%c%s%c' " % (key, '%', value, '%'))
        if len(content_list) <= 0:
            return ''

        where_content = " and ".join(content_list)
        return where_content

    def __get_default_ret_map(self, status, info):
        ret_map = {}
        ret_map["status"] = status
        ret_map["info"] = info
        return json.dumps(ret_map)

    def __show_task_history(
            self,
            owner_id,
            page_min,
            page_max,
            order_field,
            sort_order,
            where_content):
        count, run_historys = self.__sql_manager.show_task_history(
                owner_id,
                page_min,
                page_max,
                order_field,
                sort_order,
                where_content)
        return self.__get_orignal_history_ret(count, run_historys)

    def __get_orignal_history_ret(self, count, run_historys):
        if run_historys is None:
            ret_map = {}
            ret_map["status"] = 1
            ret_map["info"] = "visit mysql failed! please check db!"
            return json.dumps(ret_map)

        history_list = []
        for history in run_historys:
            his_map = {}
            his_map["id"] = history[9]
            his_map["task_id"] = history[0]
            his_map["run_time"] = history[1]
            his_map["pl_id"] = history[2]
            his_map["start_time"] = history[3].strftime("%Y-%m-%d %H:%M:%S")
            his_map["status"] = history[5]
            his_map["schedule_id"] = history[6]
            his_map["pl_name"] = history[7]
            his_map["task_name"] = history[8]
            use_time = history[4] - history[3]
            his_map["use_time"] = use_time.days * 24 * 3600 + use_time.seconds
            his_map["cpu"] = history[10]
            his_map["mem"] = history[11]
            history_list.append(his_map)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["count"] = count
        ret_map["runhistory_list"] = history_list
        return json.dumps(ret_map)

    def get_run_history_info(self, run_history_id):
        run_history = self.__sql_manager.get_run_history_info(run_history_id)
        if run_history is None:
            return self.__get_default_ret_map(1, "db error!")
        his_map = {}
        his_map["id"] = run_history.id
        his_map["task_id"] = run_history.task_id
        his_map["schedule_id"] = run_history.schedule_id
        his_map["run_time"] = run_history.run_time
        his_map["pl_id"] = run_history.pl_id
        his_map["begin_time"] = run_history.start_time.strftime(
                "%Y-%m-%d %H:%M:%S")
        his_map["end_time"] = run_history.end_time.strftime(
                "%Y-%m-%d %H:%M:%S")
        his_map["status"] = run_history.status
        his_map["pl_name"] = run_history.pl_name
        his_map["task_name"] = run_history.task_name
        his_map["status"] = run_history.status
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["runhistory"] = his_map
        return json.dumps(ret_map)

    def get_task_run_status_info(self, task_id, run_time):
        try:
            run_history = horae.models.RunHistory.objects.get(
                task_id=task_id,
                run_time=run_time)
            if run_history.status != tools_util.TaskState.TASK_WAITING:
                ret_map = {}
                ret_map["status"] = 0
                ret_map["info"] = tools_util.global_status_info_map[
                    run_history.status]
                if run_history.status == tools_util.TaskState.TASK_RUNNING:
                    if self.__pipeline_mgr is not None:
                        admin_ip, admin_port = \
                            self.__pipeline_mgr.get_admin_ip_port()
                        if admin_ip is not None and admin_port is not None:
                            unique_id = uuid.uuid1()
                            while not tools_util.CONSTANTS.GLOBAL_STOP:
                                node_req_url = ("http://%s:%s/get_proceeding?"
                                                "unique_id=%s&schedule_id=%s" % (
                                                    admin_ip,
                                                    admin_port,
                                                    unique_id,
                                                    run_history.schedule_id))
                                url_stream = "FAIL"
                                try:
                                    url_stream = urllib.request.urlopen(
                                        node_req_url).read()
                                except Exception as ex:
                                    url_stream = str(ex)

                                if url_stream == tools_util.CONSTANTS.HTTP_RESPONSE_WAIT:
                                    time.sleep(1)
                                    continue

                                ret_map["info"] = url_stream
                                break

                ret_map["prev_info"] = {}
                return json.dumps(ret_map)
        except Exception as ex:
            return self.__get_default_ret_map(1, str(ex))
        task = self.__sql_manager.get_task_info(task_id)
        if task is None:
            return self.__get_default_ret_map(1, 'db error!')
        graph = self.__graph_mgr.get_graph(str(task.id))
        if graph is None:
            return self.__get_default_ret_map(
                1,
                "graph is not exists!%s" % task.id)

        pre_node_list = []
        self.__graph_mgr.get_all_preccessors_recurrence(
            graph,
            str(task.id),
            pre_node_list)
        if len(pre_node_list) <= 0:
            ret_map = {}
            ret_map["status"] = 0
            ret_map["info"] = "no prev nodes, waiting to be called!"
            ret_map["prev_info"] = {}
            return json.dumps(ret_map)
        tmp_id_list = copy.deepcopy(pre_node_list)
        tmp_id_list.append(str(task.id))
        prev_max_ct_time, prev_max_task_id = self.__get_max_prev_ct_time(
            tmp_id_list)

        status, un_succ_tasks = self.__get_prev_unsucc_tasks(
            run_time,
            pre_node_list,
            prev_max_ct_time)
        if not status:
            return self.__get_default_ret_map(
                1,
                "get prev un succ tasks failed!")

        status_map = {}
        for un_succ_task in un_succ_tasks:
            task_map = {}
            task_map["id"] = un_succ_task.id
            task_map["pl_id"] = un_succ_task.pl_id
            task_map["task_id"] = un_succ_task.task_id
            task_map["run_time"] = un_succ_task.run_time
            task_map["start_time"] = un_succ_task.start_time.strftime(
                "%Y-%m-%d %H:%M:%S")
            task = self.__sql_manager.get_task_info(un_succ_task.task_id)
            owner_list, pipeline = self.__sql_manager.get_pipeline_info(
                un_succ_task.pl_id)
            managers = self.__sql_manager.get_manager_info_list(owner_list)
            if managers is not None:
                user_list = []
                for manager in managers:
                    user_list.append((manager.username, manager.email))
                task_map["manager_list"] = user_list

            task_map["task_name"] = task.name
            task_map["pipeline_name"] = pipeline.name

            if str(un_succ_task.status) not in status_map:
                tmp_list = []
                tmp_list.append(task_map)
                status_map[str(un_succ_task.status)] = tmp_list
            else:
                status_map[str(un_succ_task.status)].append(task_map)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        if len(un_succ_tasks) <= 0:
            ret_map["info"] = (
                "prev nodes are all succ, "
                "wait to schedule by admin, "
                "may by system busy or no work node!")
        ret_map["prev_info"] = status_map
        return json.dumps(ret_map)

    def __get_prev_unsucc_tasks(
            self,
            run_time,
            prev_nodes,
            max_ct_time):
        if prev_nodes is None or len(prev_nodes) <= 0:
            return True, []

        if max_ct_time is None:
            return True, self.__sql_manager.get_run_history_with_task_ids(
                prev_nodes,
                run_time,
                run_time)

        max_ct_time = tools_util.StaticFunction.strip_with_one_space(
            max_ct_time)
        ct_time_list = max_ct_time.split(' ')
        ct_time_len = len(ct_time_list)
        if ct_time_len != tools_util.CONSTANTS.CT_TIME_SPLIT_NUM:
            self.__log.error("pipeline ct time error!")
            return False, None

        max_ct_time_pos = -1
        for i in range(ct_time_len - 1, -1, -1):
            if ct_time_list[i] != '*':
                max_ct_time_pos = i
                break

        if max_ct_time_pos == -1:
            return False, None

        check_equal_level_list = []
        check_slow_level_list = []

        prev_nodes_str = ','.join(prev_nodes)
        prev_nodes_with_ct_time = self.__sql_manager.get_prev_nodes_info(
            prev_nodes_str)

        for node in prev_nodes_with_ct_time:
            if node[1] is None:
                check_slow_level_list.append(node[0])
                continue

            ct_time = node[1].strip()
            if ct_time == '':
                check_slow_level_list.append(node[0])
                continue

            tmp_ct_time_list = ct_time.split(' ')
            if len(tmp_ct_time_list) != tools_util.CONSTANTS.CT_TIME_SPLIT_NUM:
                self.__log.error("pipeline ct time error![%s]" % ct_time)
                return False, None

            if tmp_ct_time_list[max_ct_time_pos] == '*':
                check_slow_level_list.append(node[0])
            else:
                check_equal_level_list.append(node[0])

        middle_run_time = self.__get_run_time_period_min(
            run_time,
            max_ct_time,
            1)

        un_succ_tasks = []
        if len(check_slow_level_list) > 0:
            min_run_time = self.__get_run_time_period_min(
                run_time,
                max_ct_time)

            tmp_unsucc_tasks = self.__sql_manager.get_run_history_with_task_ids(
                check_slow_level_list,
                min_run_time,
                run_time)
            for task in tmp_unsucc_tasks:
                un_succ_tasks.append(task)

        if len(check_equal_level_list) > 0:
            tmp_unsucc_tasks = self.__sql_manager.get_run_history_with_task_ids(
                check_equal_level_list,
                middle_run_time,
                run_time)
            for task in tmp_unsucc_tasks:
                un_succ_tasks.append(task)

        return True, un_succ_tasks

    def __max_ct_time(self, lhs_item, rhs_item):
        lhs_ct_time = tools_util.StaticFunction.strip_with_one_space(lhs_item)
        lhs_ct_time_list = lhs_ct_time.split(" ")
        if len(lhs_ct_time_list) != tools_util.CONSTANTS.CT_TIME_SPLIT_NUM:
            return rhs_item
        rhs_ct_time = tools_util.StaticFunction.strip_with_one_space(rhs_item)
        rhs_ct_time_list = rhs_ct_time.split(" ")
        if len(rhs_ct_time_list) != tools_util.CONSTANTS.CT_TIME_SPLIT_NUM:
            return lhs_item

        for i in range(tools_util.CONSTANTS.CT_TIME_SPLIT_NUM - 1, -1, -1):
            if lhs_ct_time_list[i] == '*' and rhs_ct_time_list[i] == '*':
                continue

            if lhs_ct_time_list[i] != '*' and rhs_ct_time_list[i] != '*':
                lhs_sub_ct_time_list = lhs_ct_time_list[i].split(',')
                rhs_sub_ct_time_list = rhs_ct_time_list[i].split(',')

                if len(lhs_sub_ct_time_list) > len(rhs_sub_ct_time_list):
                    return rhs_item

                if len(lhs_sub_ct_time_list) < len(rhs_sub_ct_time_list):
                    return lhs_item

                if lhs_ct_time_list[i] > rhs_ct_time_list[i]:
                    return lhs_item
                elif lhs_ct_time_list[i] < rhs_ct_time_list[i]:
                    return rhs_item
                continue

            if lhs_ct_time_list[i] != '*':
                return lhs_item
            return rhs_item
        return lhs_item

    def __get_max_prev_ct_time(self, prev_node_list):
        if prev_node_list is None:
            return None, None
        max_ct_time = '* * * * *'
        max_task_id = None
        tasks = horae.models.Task.objects.filter(id__in=prev_node_list)
        pl_id_list = []
        task_id_map = {}
        for task in tasks:
            pl_id_list.append(task.pl_id)
            task_id_map[str(task.id)] = task

        pipelines = horae.models.Pipeline.objects.filter(id__in=pl_id_list)
        pipeline_map = {}
        for pipe in pipelines:
            pipeline_map[pipe.id] = pipe

        for task_id in prev_node_list:
            try:
                task = task_id_map[str(task_id)]
                pipeline = pipeline_map[task.pl_id]
                tmp_max_ct_time = self.__max_ct_time(
                        max_ct_time,
                        pipeline.ct_time)
                if tmp_max_ct_time == max_ct_time:
                    continue
                max_ct_time = tmp_max_ct_time
                max_task_id = task_id
            except Exception as ex:
                return None, None

        if max_ct_time == '* * * * *' or max_task_id is None:
            return None, None

        return max_ct_time, max_task_id
    
    def __get_run_time_period_min(self, run_time, ct_time, delta_minutes=-1):
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
            self.__log.error("job set cron_express[%s] failed!" % ct_time)
            return None
        return crontab_job.schedule(run_datetime).get_prev().strftime(
                self.__get_min_ct_time(ct_time.strip()))

    # 判断上游依赖在时间段内完成的规则
    def __get_min_ct_time(self, ct_time):
        ct_time = tools_util.StaticFunction.strip_with_one_space(ct_time)
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

    def run_history_show_graph(self, pipeline_id, run_time):
        run_historys = self.__sql_manager.get_task_history_by_pipe(
            pipeline_id,
            run_time)
        if run_historys is None:
            return self.__get_default_ret_map(1, "db error!")

        task_id_list = []
        for history in run_historys:
            task_id_list.append(str(history[0]))

        check_list = []
        task_id_set = set()
        task_map = {}
        for history in run_historys:
            task_id_set.add(history[0])
            has_prev_node = False
            if history[10] is not None and history[10].strip() != '':
                prev_task_ids = history[10].split(',')
                for prev_id in prev_task_ids:
                    if prev_id.strip() == '':
                        continue

                    if prev_id.strip() not in task_id_list:
                        has_prev_node = True
                        break

            if not has_prev_node:
                continue
            graph = self.__graph_mgr.get_graph(str(history[0]))
            if graph is None:
                return self.__get_default_ret_map(
                    1,
                    "graph is not exists![node:%s]" % history[0])

            pre_node_list = []
            self.__graph_mgr.get_all_preccessors_recurrence(
                graph,
                str(history[0]),
                pre_node_list)
            if len(pre_node_list) <= 0:
                continue

            tmp_id_list = copy.deepcopy(pre_node_list)
            tmp_id_list.append(str(history[0]))
            prev_max_ct_time, prev_max_task_id = self.__get_max_prev_ct_time(
                tmp_id_list)

            min_run_time = run_time
            if prev_max_ct_time is not None:
                min_run_time = self.__get_run_time_period_min(
                    run_time,
                    prev_max_ct_time)
            id_list_str = ','.join(pre_node_list)
            tasks = self.__sql_manager.get_tasks_by_task_ids(id_list_str)
            if tasks is None:
                return self.__get_default_ret_map(1, "db error!")
            for task in tasks:
                tmp_list = []
                task_map[task[0]] = (task, tmp_list)
                task_id_set.add(task[0])

            # 获取run_time区间内所有非成功的任务
            un_succ_tasks = self.__sql_manager.get_run_history_with_task_ids(
                pre_node_list,
                min_run_time,
                run_time)
            for un_succ_task in un_succ_tasks:
                if un_succ_task.task_id not in task_map:
                    return self.__get_default_ret_map(1, "db error!")
                task_map[un_succ_task.task_id][1].append(un_succ_task)

        add_task_set = set()
        history_list = []
        for history in run_historys:
            if history[0] in add_task_set:
                continue
            add_task_set.add(history[0])

            his_map = {}
            his_map["task_id"] = history[0]
            his_map["run_time"] = history[1]
            his_map["pl_id"] = history[2]

            owner_list, pipeline = self.__sql_manager.get_pipeline_info(
                history[2])
            managers = self.__sql_manager.get_manager_info_list(owner_list)
            if managers is not None:
                user_list = []
                for manager in managers:
                    user_list.append((manager.username, manager.email))
                his_map["manager_list"] = user_list

            his_map["begin_time"] = history[3].strftime("%Y-%m-%d %H:%M:%S")
            his_map["status"] = history[5]
            his_map["schedule_id"] = history[6]
            his_map["pl_name"] = history[7]
            his_map["task_name"] = history[8]
            next_task_id_list = []
            edges = horae.models.Edge.objects.filter(prev_task_id=history[0])
            for edge in edges:
                if edge.next_task_id in task_id_set:
                    next_task_id_list.append(str(edge.next_task_id))

            his_map["next_task_ids"] = ','.join(next_task_id_list)
            his_map["prev_task_ids"] = history[10]
            his_map["retried_count"] = 0
            his_map["un_succ_run_tasks"] = []
            history_list.append(his_map)

        for prev_task in task_map:
            task = task_map[prev_task][0]
            if task[0] in add_task_set:
                continue
            add_task_set.add(task[0])

            his_map = {}
            his_map["task_id"] = task[0]
            his_map["run_time"] = ''
            his_map["pl_id"] = task[1]
            owner_list, pipeline = self.__sql_manager.get_pipeline_info(
                task[1])
            managers = self.__sql_manager.get_manager_info_list(owner_list)
            if managers is not None:
                user_list = []
                for manager in managers:
                    user_list.append((manager.username, manager.email))
                his_map["manager_list"] = user_list

            his_map["begin_time"] = ''
            his_map["status"] = tools_util.TaskState.TASK_SUCCEED
            his_map["schedule_id"] = -1
            his_map["pl_name"] = task[8]
            his_map["task_name"] = task[6]
            next_task_ids = task[3].split(',')
            next_task_id_list = []
            edges = horae.models.Edge.objects.filter(prev_task_id=task[0])
            for edge in edges:
                if edge.next_task_id in task_id_set:
                    next_task_id_list.append(str(edge.next_task_id))

            # for task_id in next_task_ids:
            #     if task_id.strip() == '':
            #         continue
            #     if int(task_id) in task_id_set:
            #         next_task_id_list.append(task_id.strip())

            his_map["next_task_ids"] = ','.join(next_task_id_list)
            his_map["prev_task_ids"] = task[4]
            his_map["retried_count"] = task[7]
            un_succ_task_list = []
            if len(task_map[prev_task][1]) > 0:
                for un_succ_task in task_map[prev_task][1]:
                    un_succ_task_map = {}
                    un_succ_task_map["run_time"] = un_succ_task.run_time
                    un_succ_task_map["status"] = un_succ_task.status
                    un_succ_task_list.append(un_succ_task_map)
                    if un_succ_task.status == \
                            tools_util.TaskState.TASK_FAILED \
                            or un_succ_task.status == \
                            tools_util.TaskState.TASK_STOPED_BY_USER:
                        his_map["status"] = un_succ_task.status
                    elif his_map["status"] == \
                            tools_util.TaskState.TASK_SUCCEED:
                        his_map["status"] = un_succ_task.status
            his_map["un_succ_run_tasks"] = un_succ_task_list
            history_list.append(his_map)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["runhistory_list"] = history_list
        return json.dumps(ret_map)
