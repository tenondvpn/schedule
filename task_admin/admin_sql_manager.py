###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

from asyncio import exceptions
import sys
import os
import traceback
import logging

import django.db
import horae.models
import django.db.models
import django.core.exceptions

sys.path.append('../common')
import task_util

class ConstantSql(object):
    TASK_WITH_PIPEID_SQL1 = (
            "select a.id, a.pl_id, a.next_task_ids, "
            "a.prev_task_ids, a.pid, a.server_tag, b.type, a.retry_count, "
            "a.name, a.last_run_time "
            "from ( "
            "    select id, pl_id, next_task_ids, prev_task_ids, "
            "    pid, retry_count, name, server_tag, last_run_time "
            "    from horae_task "
            ") a "
            "left outer join ( "
            "    select id, type from horae_processor "
            ") b "
            "on a.pid = b.id where b.id is not null;")
    TASK_WITH_PIPEID_SQL = (
            'select a.id, a.pl_id, a.next_task_ids, a.prev_task_ids, a.pid,'
            ' a.server_tag, b.type, a.retry_count, a.name, a.last_run_time'
            ' from horae_task a left outer join horae_processor b '
            'on a.pid = b.id where b.id is not null;')
    GET_MIN_RUNTIME_USER_SCHEDULE_TASKS = (
            "select distinct ordered_id "
            "from horae_orderdschedule;")

class SqlManager(object):
    def __init__(self):
        self.__log = logging

    # 从schedule，task表获取所有的可调用的任务
    def get_all_tasks(self, tasks_map, pipeline_map, pipe_task_map):
        try:
            pipelines = horae.models.Pipeline.objects.all()
            for pipeline in pipelines: 
                pipeline_map[pipeline.id] = pipeline
            cursor = django.db.connection.cursor()
            cursor.execute(ConstantSql.TASK_WITH_PIPEID_SQL)
            task_records = cursor.fetchall()
            for task in task_records:
                if task[1] not in pipeline_map:
                    continue

                tasks_map[str(task[0])] = task
                if task[1] in pipe_task_map:
                    pipe_task_map[task[1]].append(task)
                else:
                    tmp_list = []
                    tmp_list.append(task)
                    pipe_task_map[task[1]] = tmp_list
            return True
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return False
        except Exception as ex:
            self.__log.error("execute sql failed![%s][ex:%s][trace:%s]!" % (
                    ConstantSql.TASK_WITH_PIPEID_SQL,
                    str(ex),
                    traceback.format_exc()))
            return False

    def get_all_tasks_by_task_name(self, task_name_list, pl_id):
        try:
            schedules = horae.models.Schedule.objects.filter(
                    pl_id=pl_id,
                    name__in=self.__task_name_list)
            if len(schedules) <= 0:
                return schedules
            return schedules
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    # 判断schedule表中是否已经写入了每个流程的起始节点
    def task_has_inserted_to_schedule(self, task_id, run_time):
        try:
            task = horae.models.Schedule.objects.get(
                    task_id=task_id, 
                    run_time=run_time)
            if task is None:
                return False
            return True
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return False
        except django.core.exceptions.ObjectDoesNotExist:
            return False
        except django.core.exceptions.FieldDoesNotExist:
            return False
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return False  # 写入的时候会失败，所以即使异常也不影响

    # 事务性执行多个sql命令
    @django.db.transaction.atomic
    def batch_execute_with_affect_one(
            self, 
            sql_save_list=None, 
            sql_del_list=None,
            sql_src_list=None):
        try:
            with django.db.transaction.atomic():
                if sql_save_list is not None:
                    for object in sql_save_list:
                        object.save()

                if sql_del_list is not None:
                    for object in sql_del_list:
                        object.delete()

                if sql_src_list is not None:
                    cursor = django.db.connection.cursor()
                    for sql in sql_src_list:
                        change_row = cursor.execute(sql)
                        if change_row < 1:
                            raise Exception("exe %s failed, changed"
                                    " rows[%d]" % (sql, change_row))
                return True
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return False
        except Exception as ex:
            sql_str_list = ''
            if sql_src_list is not None and len(sql_src_list) > 0:
                sql_str_list = '\n'.join(sql_src_list)
            self.__log.warn("execute sql failed![sql:%s][%s][trace:%s]!" % (
                    sql_str_list, str(ex), traceback.format_exc()))
            return False

    # 防止db数据量爆发，每次只取1000条，且根据run_time先后顺序执行
    def get_all_begin_node_task_from_schedule(self, task_list):
        try:
            schedules = horae.models.Schedule.objects.filter(
                    status=task_util.TaskState.TASK_WAITING, 
                    task_id__in=task_list).order_by('run_time')[:10000]
            if len(schedules) <= 0:
                return schedules
            return schedules
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_last_task_run_time(self, task_id):
        try:
            task = horae.models.Task.objects.get(id=task_id)
            if task is None:
                return None
            return task.last_run_time
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error(str(ex) + \
                    " execute sql failed![get_last_task_run_time]!")
            return None

    def get_task_num_with_group_type(self):
        try:
            ready_tasks = horae.models.ReadyTask.objects.filter( 
                    status__in=(
                            task_util.TaskState.TASK_RUNNING, 
                            task_util.TaskState.TASK_READY)
                    ).values('type'). \
                    annotate(sum_type=django.db.models.Count('type'))
            if len(ready_tasks) <= 0:
                return ready_tasks
            return ready_tasks
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_owner_task_num_with_type(self):
        try:
            ready_tasks = horae.models.ReadyTask.objects.filter( 
                    status__in=(
                            task_util.TaskState.TASK_RUNNING, 
                            task_util.TaskState.TASK_READY)
                    ).values('type', 'owner_id'). \
                    annotate(sum_type_owener=django.db.models.Count('type'))
            if len(ready_tasks) <= 0:
                return ready_tasks
            return ready_tasks
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    # 此处必须获取全部成功的任务
    def get_all_succeeded_tasks(self):
        try:
            ready_tasks = horae.models.ReadyTask.objects.filter( 
                    status=task_util.TaskState.TASK_SUCCEED)
            if len(ready_tasks) <= 0:
                return ready_tasks
            return ready_tasks
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def check_all_task_succeeded(self, task_id_list, min_time, max_time):
        if task_id_list is None or len(task_id_list) <= 0:
            return True

        try:
            tasks = horae.models.Schedule.objects.exclude(
                        status=task_util.TaskState.TASK_SUCCEED).filter( 
                        run_time__lte=max_time,
                        run_time__gte=min_time,
                        task_id__in=task_id_list)
            if tasks is None or len(tasks) <= 0:
                return True
            return False
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return 1
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return 1

    def task_is_in_schedule_with_status(self, task_id, run_time, status):
        try:
            return horae.models.Schedule.objects.get( 
                    task_id=task_id,
                    run_time=run_time,
                    status=status)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except django.core.exceptions.ObjectDoesNotExist:
            return None
        except django.core.exceptions.FieldDoesNotExist:
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def task_is_in_schedule_no_status(self, task_id, run_time):
        try:
            return horae.models.Schedule.objects.get( 
                    task_id=task_id,
                    run_time=run_time)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except django.core.exceptions.ObjectDoesNotExist:
            return None
        except django.core.exceptions.FieldDoesNotExist:
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_owner_id_list(self, source_id, source_name='pipeline'):
        OWNER_ID_LIST_SQL = (
            "select max(id) from horae_permhistory where "
            "resource_type = '%s' and resource_id = %d group by "
            "applicant_id;")

        class UserPermissionType(object):
            NO_AUTH = 0
            READ = 1
            WRITE = 2
            CONFIRMING = 3  # 申请权限确认中
            READ_STR = "read"
            WRITE_STR = "write"

        class AuthAction(object):
            GRANT_AUTH_TO_OTHER = 0  # 授予权限给其他用户
            CONFIRM_APPLY_AUTH = 1  # 用户确认其他用户的权限申请
            REJECT_APPLY_AUTH = 2  # 用户拒绝其他用户的权限申请
            APPLY_AUTH = 3  # 用户申请权限

        tmp_sql = OWNER_ID_LIST_SQL % (source_name, source_id)
        self.__log.info("run sql: %s" % tmp_sql)
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            self.__log.info("success run sql: %s, rows: %d" % (tmp_sql, len(rows)))
            id_list = []
            for row in rows:
                id_list.append(int(row[0]))

            if len(id_list) <= 0:
                return [], []

            perm_historys = horae.models.PermHistory.objects.filter(
                id__in=id_list,
                permission__in=(
                    UserPermissionType.WRITE_STR,
                    UserPermissionType.READ_STR),
                status__in=(
                    AuthAction.CONFIRM_APPLY_AUTH,
                    AuthAction.GRANT_AUTH_TO_OTHER))
            read_res_id_list = []
            write_res_id_list = []
            for perm in perm_historys:
                if perm.applicant_id is None:
                    continue

                if perm.permission == UserPermissionType.WRITE_STR:
                    write_res_id_list.append(perm.applicant_id)
                elif perm.permission == UserPermissionType.READ_STR:
                    read_res_id_list.append(perm.applicant_id)
                else:
                    pass
            return read_res_id_list, write_res_id_list
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return None, None
        
    def get_all_unexp_tasks(self):
        try:
            ready_tasks = horae.models.ReadyTask.objects.filter( 
                    status=task_util.TaskState.TASK_FAILED)
            if len(ready_tasks) <= 0:
                return ready_tasks
            return ready_tasks
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    @django.db.transaction.atomic
    def stop_pipeline(self, pipeline_id, run_time):
        try:
            with django.db.transaction.atomic():
                schedules = horae.models.Schedule.objects.filter(
                        pl_id=pipeline_id, 
                        run_time=run_time)
                for schedule in schedules:
                    # 有schedule必然有run_history，否则失败
                    if schedule.status not in (
                            task_util.TaskState.TASK_READY, 
                            task_util.TaskState.TASK_RUNNING, 
                            task_util.TaskState.TASK_TIMEOUT, 
                            task_util.TaskState.TASK_WAITING):
                        continue

                    history = horae.models.RunHistory.objects.get(
                        schedule_id=schedule.id, 
                        task_id=schedule.task_id, 
                        run_time=schedule.run_time)
                    history.status=task_util.TaskState.TASK_STOPED_BY_USER
                    history.save()
                    schedule.status=task_util.TaskState.TASK_STOPED_BY_USER
                    schedule.save()

                horae_tasks = horae.models.ReadyTask.objects.filter(
                        pl_id=pipeline_id,
                        run_time=run_time)
                for ready_task in horae_tasks:
                    ready_task.status=task_util.TaskState.TASK_STOPED_BY_USER
                    ready_task.save()
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

    def get_task_id_list_with_pl_id_and_run_time(self, pl_id, run_time):
        try:
            schedules = horae.models.Schedule.objects.filter(
                            pl_id=pl_id, 
                            run_time=run_time).values("task_id")
            if len(schedules) <= 0:
                return schedules
            return schedules
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_all_ready_task_with_pl_id_and_runtime(self, pl_id, run_time):
        try:
            ready_tasks = horae.models.ReadyTask.objects.filter(
                    pl_id=pl_id, 
                    run_time=run_time)
            if len(ready_tasks) <= 0:
                return ready_tasks
            return ready_tasks
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def has_schedule_with_id_run_time(self, task_id, run_time):
        try:
            return horae.models.Schedule.objects.get(
                    task_id=task_id, 
                    run_time=run_time)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except django.core.exceptions.ObjectDoesNotExist:
            return None
        except django.core.exceptions.FieldDoesNotExist:
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_runhsitory_with_id(self, schedule_id):
        try:
            return horae.models.RunHistory.objects.get(
                    schedule_id=schedule_id)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s]"
                    "[trace:%s][schedule_id: %s]!" % (
                    str(ex), traceback.format_exc(), schedule_id))
            return None

    def get_rerunhsitory_with_id(self, rerun_id):
        try:
            return horae.models.RerunHistory.objects.get(
                    id=rerun_id)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s]"
                    "[trace:%s][schedule_id: %s]!" % (
                    str(ex), traceback.format_exc(), rerun_id))
            return None

    def get_all_waiting_tasks(self, task_set):
        try:
            schedules = horae.models.Schedule.objects.filter( 
                    status=task_util.TaskState.TASK_WAITING).order_by(
                    'run_time')
            for schedule in schedules:
                item = "%s_%s" % (schedule.task_id, schedule.run_time)
                task_set.add(item)
            return schedules
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    @django.db.transaction.atomic
    def stop_task_with_schedule_id(self, schedule_id):
        try:
            with django.db.transaction.atomic():
                schedule = horae.models.Schedule.objects.get(id=schedule_id)
                if schedule.status not in (
                        task_util.TaskState.TASK_READY, 
                        task_util.TaskState.TASK_RUNNING, 
                        task_util.TaskState.TASK_TIMEOUT, 
                        task_util.TaskState.TASK_WAITING,
                        task_util.TaskState.TASK_STOPED_BY_USER):
                    return True

                schedule.status=task_util.TaskState.TASK_STOPED_BY_USER
                run_history = horae.models.RunHistory.objects.get(
                        task_id=schedule.task_id, 
                        run_time=schedule.run_time)
                run_history.status=task_util.TaskState.TASK_STOPED_BY_USER
                schedule.save()
                run_history.save()

                ready_tasks = horae.models.ReadyTask.objects.filter(
                        schedule_id=schedule_id)
                if len(ready_tasks) <= 0:
                    return True

                ready_task = ready_tasks[0]
                ready_task.delete()
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

    def get_all_user_schedule_order_id(self):
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(ConstantSql.GET_MIN_RUNTIME_USER_SCHEDULE_TASKS)
            task_records = cursor.fetchall()
            len(task_records)
            return task_records
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_min_run_time_user_user_schedule_task(self, ordered_id):
        try:
            user_schedules = horae.models.OrderdSchedule.objects.filter(
                    ordered_id=ordered_id).order_by('run_time')[0: 1]
            if len(user_schedules) <= 0:
                return None

            return user_schedules[0]
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_ordered_schedules(self, ordered_id, run_tag):
        try:
            user_schedules = horae.models.OrderdSchedule.objects.filter(
                    ordered_id=ordered_id, run_tag=run_tag)
            len(user_schedules)
            return user_schedules
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_ordered_schedule(self, task_id, run_time):
        try:
            user_schedule = horae.models.OrderdSchedule.objects.get(
                    task_id=task_id, run_time=run_time)
            return user_schedule
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return None
        except Exception as ex:
            return None
        
    def get_user_info(self, user_id_list):
        res_list = []
        try:
            users = horae.models.UserInfo.objects.filter(
                    userid__in=user_id_list).order_by('userid')
            if len(users) <= 0:
                return res_list
            
            got_users = []
            for user in users:
                got_users.append(user.userid)

            self.__log.info("check_unexpect_status_tasks get users: %d" % len(users))
            user_details = django.contrib.auth.models.User.objects.filter(
                    id__in=got_users).order_by('id')
            self.__log.info("check_unexpect_status_tasks get users details: %d" % len(user_details))
            if len(users) != len(user_details):
                self.__log.info("check_unexpect_status_tasks get users failed: %d, %d" % (len(users), len(user_details)))
                return res_list
            
            for i in range(len(users)):
                res_list.append({
                        "id": users[i].userid,
                        "email": users[i].email,
                        "dingding": users[i].dingding,
                        "name": user_details[i].username
                    })
                
            self.__log.info("check_unexpect_status_tasks get users final: %d" % len(res_list))
            return res_list
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return res_list
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return res_list

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "db_manager.settings")
    from django.core.management import execute_from_command_line
    sys.path.append('../common/db_manager')
    import django
    django.setup()

    sql_mgr = SqlManager("./conf/admin_logger.conf")
    task_map = {}
    pipe_map = {}
    sql_mgr.get_all_tasks(task_map, pipe_map)
    print(sql_mgr.task_has_inserted_to_schedule("1094", "201505051736"))
    task_list = [1194, '1096', '1097']
    tasks = sql_mgr.get_all_begin_node_task_from_schedule(task_list)
    if tasks is not None and len(tasks) > 0:
        for task in tasks:
            print(task.id)
            print(task.task_id)
            print(task.run_time)
    print("last run time :%s" % sql_mgr.get_last_task_run_time('1194'))
    task_ids = sql_mgr.get_task_num_with_group_type()
    if task_ids is not None and len(task_ids) > 0:
        print("task_ids len: %d" % len(task_ids))
        print(task_ids)

    type_owner_sum = sql_mgr.get_owner_task_num_with_type()
    if type_owner_sum is not None and len(type_owner_sum) > 0:
        print("task_ids len: %d" % len(type_owner_sum))
        print(type_owner_sum)
    print(task_map)
    print(pipe_map)
    for pipe in pipe_map:
        print(pipe_map[pipe].id)
        print(pipe_map[pipe].owner_id)
        print(pipe_map[pipe].ct_time)

    print("test transaction:")
    his_run1 = horae.models.RunHistory.objects.get(id = 1)
    his_run1.end_time = '2015-05-05 13:00:00'
    his_run1.runner_server = 'test2'
    sql_list = []
    sql_list.append(his_run1)
    his_run1 = horae.models.RunHistory.objects.get(id = 2)
    his_run1.end_time = '2015-05-05 13:00:00'
    his_run1.runner_server = 'test2'
    sql_list.append(his_run1)
    print(sql_mgr.batch_execute_with_transaction(sql_list))
