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
import qq_mail
import dingding_group

class CheckUnexpectStatusTasks(admin_task_base.AdminTaskBase):
    """
        获取非正常状态任务，从ready_task中删除失败任务，
        重试失败任务
        非正常状态写日志并报警
    """
    def __init__(self, task_node_manager):
        admin_task_base.AdminTaskBase.__init__(self)
        self.__unexp_task_list = []
        self.__log = logging
        self.__sql_manager = admin_sql_manager.SqlManager()
        self.__sql_del_list = []
        self.__sql_save_list = []
        self.__sql_list = []
        self.__task_node_manager = task_node_manager

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
                self.__log.info("CheckUnexpectStatusTasks handle data started")

                self.__handle_unexp_tasks()
                use_time = time.time() - begin_time
                self.__log.info("CheckUnexpectStatusTasks handle data "
                        "exit.use time[%f]" % use_time)
                break

            time.sleep(10)  # 失败后任然需要sleep

        self.__log.info("CheckUnexpectStatusTasks thread existed!")

    def __retry_failed_task(self, unexp_task):
        # 检查休眠时长
        if not self.__check_rerun_time_period(unexp_task):
            return True

        # 检查任务数限制
        owner_id = unexp_task.owner_id
        task_type = unexp_task.type
        if not self._check_limit_num_can_run(owner_id, task_type):
            self.__log.info("limit is over![type:%d]"
                    "[owner_id:%d]" % (task_type, owner_id))
            return True

        # 获取可以执行的task_node，合法性检查会通过get_valid_ip进行
        tmp_run_server = self.__task_node_manager.get_valid_ip(
                unexp_task.type, 
                unexp_task.server_tag)
        if tmp_run_server is None:
            self.__log.error("get task node run_server failed![task_id:%s]"
                    "[type:%d][server_tag:%s]" % (
                    unexp_task.task_id, 
                    unexp_task.type, 
                    unexp_task.server_tag))
            return False
        ready_task_sql = ("update horae_readytask set status = %d, "
                "retried_count = %d, run_server = '%s', update_time = '%s'"
                " where id = %d and status = %d "
                " and retried_count = %d;" % (
                task_util.TaskState.TASK_READY, 
                unexp_task.retried_count + 1, 
                tmp_run_server,
                task_util.StaticFunction.get_now_format_time(
                        "%Y-%m-%d %H:%M:%S"),
                unexp_task.id, 
                unexp_task.status, 
                unexp_task.retried_count))
        self.__sql_list.append(ready_task_sql)

        # 更新schedule
        schedule_sql = ("update horae_schedule set status = %d "
                "where id = %d and status = %d;" % (
                task_util.TaskState.TASK_READY, 
                unexp_task.schedule_id, 
                unexp_task.status))
        self.__sql_list.append(schedule_sql)

        # 写run_history
        run_history_sql = ("update horae_runhistory set status = %d, "
                "run_server = '%s', schedule_id = %d where task_id = %s "
                "and status = %d and run_time = '%s'; " % (
                task_util.TaskState.TASK_READY, 
                tmp_run_server,
                unexp_task.schedule_id,
                unexp_task.task_id,
                unexp_task.status,
                unexp_task.run_time))
        self.__sql_list.append(run_history_sql)

        # 写重试历史表
        run_history = horae.models.RunHistory.objects.get(
            task_id=unexp_task.task_id, run_time=unexp_task.run_time)
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
        return True
 
    # 任务重试给一个休眠惩罚，防止频繁重试
    def __check_rerun_time_period(self, unexp_task):
        try:
            sleep_period = 0
            # 防止溢出
            if unexp_task.retried_count >= 100:
                sleep_period = task_util.CONSTANTS.TASK_RETRY_MAX_TIME_PERIOD
            else:
                sleep_period = (
                        (task_util.CONSTANTS.TASK_RETRY_STEP_INC_RATIO ** 
                        float(unexp_task.retried_count)) * 
                        task_util.CONSTANTS.TASK_RETRY_STEP_INC_TIME)
            if sleep_period > task_util.CONSTANTS.TASK_RETRY_MAX_TIME_PERIOD:
                sleep_period = task_util.CONSTANTS.TASK_RETRY_MAX_TIME_PERIOD

            now_time = time.time()
            last_run_time = time.mktime(time.strptime(
                    str(unexp_task.update_time), 
                    '%Y-%m-%d %H:%M:%S'))
            if now_time - last_run_time >= sleep_period:
                return True
            return False
        except Exception as ex:
            self.__log.error("there is error: %s, trace: %s" % (
                    str(ex), traceback.format_exc()))
            return False
        
    def __send_monitor(self, task, unexp_task):
        pipeline = None
        try:
            pipeline = horae.models.Pipeline.objects.get(id=task.pl_id)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return
        
        if pipeline.monitor_way == -1:
            return
        
        read_list, write_list = self.__sql_manager.get_owner_id_list(task.pl_id)
        users = self.__sql_manager.get_user_info(write_list)
        names = []
        receivers = []
        dingding_receivers = []
        dingding_names = []
        for user in users:
            if user["email"] is not None and user["email"].strip() != "":
                if user["email"] not in receivers:
                    receivers.append(user["email"])

                if user["name"] not in names:
                    names.append(user["name"])

            if user["dingding"] is not None and user["dingding"].strip() != "":
                if user["dingding"] not in dingding_receivers:
                    dingding_receivers.append(user["dingding"])

                if user["name"] not in dingding_names:
                    dingding_names.append(user["name"])

        self.__log.info(f"montor way: {pipeline.monitor_way}, users: {users}, {(pipeline.monitor_way & 1 == 1)}, receivers: {receivers}, write_list: {write_list}")
        if pipeline.monitor_way & 1 == 1:
            header = "Databaas监控报警"
            sub_header = "流程: %s, 任务: %s 执行失败！" % (pipeline.name, task.name)
            content = ("<b>%s:</b>\n <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;流程: <b>%s</b><br> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;任务：<b>%s</b><br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;运行时间点：<b>%s</b> <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<font color='red'>执行失败了!</font>\n<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;请登录平台检查: <b>http://82.156.224.174:8004/runing/%d/</b>" % 
            (", ".join(names), pipeline.name, task.name, unexp_task.run_time, task.pl_id))
            if pipeline.type == 2:
                header = "华康能管监控报警"
                sub_header = "监测组: %s, 监测策略: %s 失败！" % (pipeline.name, task.name)
                content = ("<b>%s:</b>\n <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;监测组: <b>%s</b><br> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;监测策略<b>%s</b><br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;监测时间点：<b>%s</b> <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<font color='red'>监测失败!</font>\n<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;请登录平台检查: <b>http://82.156.224.174:8004/runing/%d/</b>" % 
                (", ".join(names), pipeline.name, task.name, unexp_task.run_time, task.pl_id))

            qq_mail.send_mail(receivers, header, sub_header, content)

        if pipeline.monitor_way & 4 == 1:
            content = "%s：\n    执行失败!\n    流程: %s\n    任务: %s\n    运行时间点: %s\n    请登录平台检查: http://82.156.224.174:8004/runing/%d/" % (
                    ", ".join(dingding_names), 
                    pipeline.name, task.name, unexp_task.run_time, task.pl_id)
                
            for dingding_url in dingding_receivers:
                dingding_group.send_dingding_group(dingding_url, content)

    def __handle_failed_tasks(self, unexp_task):
        task = None
        try:
            tasks = horae.models.Task.objects.filter(id=unexp_task.task_id)
            if len(tasks) <= 0:
                self.__sql_list.append("delete from horae_readytask where task_id=%d;" % unexp_task.task_id)
                self.__sql_list.append("delete from horae_rerunhistory where task_id=%d;" % unexp_task.task_id)
                self.__sql_list.append("delete from horae_runhistory where task_id=%d;" % unexp_task.task_id)
                self.__sql_list.append("delete from horae_schedule where task_id=%d;" % unexp_task.task_id)
                return True

            task = tasks[0] 
            if task.retry_count == -1:  # 一直重试
                return self.__retry_failed_task(unexp_task)

            if unexp_task.retried_count < task.retry_count:
                return self.__retry_failed_task(unexp_task)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc(),
                    unexp_task.task_id))
            return False
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s], task_id: %d!" % (
                    str(ex), traceback.format_exc()))
            return False
        
        # monitor
        self.__send_monitor(task, unexp_task)
        self.__sql_del_list.append(unexp_task)
        return True
    
    # 获取失败状态的任务，重试或者从ready_task中删除
    def __handle_unexp_tasks(self):
        self.__unexp_task_list = self.__sql_manager.get_all_unexp_tasks()
        if self.__unexp_task_list is None or len(self.__unexp_task_list) <= 0:
            return True

        for unexp_task in self.__unexp_task_list:
            self.__sql_del_list = []
            self.__sql_save_list = []
            self.__sql_list = []

            if unexp_task.status == task_util.TaskState.TASK_FAILED:
                if not self.__handle_failed_tasks(unexp_task):
                    self.__log.info("handle failed task failed![task_id:%d]"
                            "[run_time:%s]" % (
                            unexp_task.task_id, unexp_task.run_time))
                    continue
            else:
                continue

            if not self.__sql_manager.batch_execute_with_affect_one(
                    self.__sql_save_list, 
                    self.__sql_del_list,
                    self.__sql_list):
                self.__log.error("save or del db data failed!")
        return True

if __name__ == "__main__":
    for retried_count in range(0, 100):
        sleep_period = 0
        # 防止溢出
        if unexp_task.retried_count >= 100:
            sleep_period = task_util.CONSTANTS.TASK_RETRY_MAX_TIME_PERIOD
        else:
            sleep_period = (
                    (task_util.CONSTANTS.TASK_RETRY_STEP_INC_RATIO ** 
                    float(retried_count)) * 
                    task_util.CONSTANTS.TASK_RETRY_STEP_INC_TIME)
        if sleep_period > task_util.CONSTANTS.TASK_RETRY_MAX_TIME_PERIOD:
            sleep_period = task_util.CONSTANTS.TASK_RETRY_MAX_TIME_PERIOD
        print(sleep_period)

    print("please run test in db_manager!")
