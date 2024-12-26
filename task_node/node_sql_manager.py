###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import time
import traceback
import logging
import configparser

sys.path.append('../common')
import task_util

import django.db
import horae.models
import django.db.models
import django.core.exceptions


class ConstantSql(object):
    ALL_READY_TASK_SQL = (
        "select d.id, d.pl_id, d.schedule_id, d.type, "
        "d.task_id, d.run_time, d.pid, d.config, "
        "e.template, d.priority, d.except_ret, d.over_time, "
        "d.status, d.task_handler, d.owner_id, d.work_dir, "
        "d.retried_count, d.retry_count, d.version_id, d.init_time, d.task_name "
        "from ( "
        "    select a.id, a.pl_id, a.schedule_id, a.type, a.run_time, "
        "    a.task_id, a.pid, a.status, a.task_handler, a.owner_id, "
        "    a.work_dir, a.retried_count, a.retry_count, "
        "    b.config, b.priority, b.except_ret, b.over_time, b.version_id, a.init_time, b.task_name from( "
        "        select "
        "           id, pl_id, schedule_id, type, run_time, "
        "           task_id, pid, status, task_handler, "
        "           owner_id, work_dir, retried_count, retry_count, init_time "
        "        from horae_readytask  "
        "        where run_server = %s and status in (%s, %s, %s, %s, %s) "
        "     ) a "
        "    left outer join( "
        "        select id, config, priority, except_ret, over_time, version_id, name as task_name "
        "        from horae_task "
        "    )b on a.task_id = b.id where b.id is not null "
        ") d "
        "left outer join( "
        "    select id, template from horae_processor "
        ") e "
        "on d.pid = e.id where e.id is not null order by d.run_time asc; ")
    ALL_READY_TASK_SQL_PARAM_NUM = 21  # 这个字段必须和
    # ALL_READY_TASK_SQL获取的字段数一致

    GET_READY_TASK_BY_SCHEDULE_ID_SQL = (
        "select a.id, a.pl_id, a.schedule_id, a.type, "
        "a.task_id, a.run_time, a.pid, b.config, "
        " '' as template, b.priority, b.except_ret, b.over_time, "
        "a.status, a.task_handler, a.owner_id, a.work_dir, "
        "0 as retried_count, 0 as retry_count, b.version_id, a.start_time as init_time, a.task_name as task_name from( "
        "    select id, pl_id, schedule_id, type, run_time, "
        "    task_id, 0 as pid, status, task_handler, "
        "    0 as owner_id, '%s' as work_dir, start_time, task_name "
        "    from horae_runhistory "
        "    where schedule_id = %s "
        ") a left outer join( "
        "    select id, config, priority, except_ret, over_time, version_id "
        "    from horae_task "
        ")b on a.task_id = b.id where b.id is not null;")

    UPDATE_ALL_READY_TASK = ("update horae_readytask "
                             "set status=%s where run_server=%s and status = %s;")

    OWNER_ID_LIST_SQL = ("select max(id) from common_permhistory where "
                         "resource_type = 'pipeline' and resource_id = %d group by "
                         "applicant_id;")


class SqlManager(object):
    def __init__(self):
        self.__log = logging
        self.__config = configparser.ConfigParser()
        self.__config.read("./conf/node.conf")

    # 从schedule，task表获取所有的可调用的任务
    def get_all_ready_tasks(self, local_ip):
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(
                ConstantSql.ALL_READY_TASK_SQL, [
                    local_ip,
                    task_util.TaskState.TASK_V100_PENDING,
                    task_util.TaskState.TASK_V100_RUNING,
                    task_util.TaskState.TASK_READY,
                    task_util.TaskState.TASK_RUNNING,
                    task_util.TaskState.TASK_TIMEOUT])
            task_records = cursor.fetchall()
            if task_records is None or len(task_records) <= 0:
                return None
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

    # 通过schedule_id获取任务信息
    def get_task_info_with_schedule_id(self, work_dir, schedule_id):
        tmp_sql = ConstantSql.GET_READY_TASK_BY_SCHEDULE_ID_SQL % (
            work_dir,
            schedule_id)
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            task_records = cursor.fetchall()
            if task_records is None or len(task_records) <= 0:
                return None
            return task_records[0]
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex),
                traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return None

    # 事务性执行多个sql命令
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
                        if change_row != 1:
                            # raise Exception
                            self.__log.error(
                                "exe %s failed[changed row[%s]" % (
                                    sql, change_row))
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

    def update_all_ready_task_to_failed(self, local_ip):
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(
                ConstantSql.UPDATE_ALL_READY_TASK, [
                    task_util.TaskState.TASK_FAILED,
                    local_ip,
                    task_util.TaskState.TASK_RUNNING])
            return True
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex),
                traceback.format_exc()))
            return False
        except Exception as ex:
            tmp_sql = ConstantSql.UPDATE_ALL_READY_TASK
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return False

    def delete_user_stopped_task_with_id(self, id):
        try:
            horae.models.ReadyTask.objects.get(id=id).delete()
            return True
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex),
                traceback.format_exc()))
            return False
        except Exception as ex:
            self.__log.error("delete_user_stopped_task_with_schedule_id"
                             " failed![ex:%s][trace:%s]!" % (
                                 str(ex), traceback.format_exc()))
            return False

    def get_datapath_by_dataname(self, data_name, data_type):
        try:
            data_info = dms.models.Config.objects.get(
                name=data_name,
                type=data_type)
            if data_info is None:
                return None
            return data_info.path
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

    def get_odps_access_info_by_userid(self, user_id):
        try:
            ark_user_info = ark_user.models.Info.objects.get(user_id=user_id)
            return True, ark_user_info
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex),
                traceback.format_exc()))
            time.sleep(10)  # 避免异常
            return self.get_odps_access_info_by_userid(user_id)
        except django.core.exceptions.ObjectDoesNotExist:
            return True, None
        except Exception as ex:
            self.__log.error("get odps access failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            self.__close_old_connections(str(ex))
            return False, None

    def get_task_num_with_group_type(self):
        try:
            public_ip = self.__config.get("node", "public_ip")
            if public_ip is None or public_ip.strip() == "":
                public_ip = task_util.StaticFunction.get_local_ip()

            sql = (
                    "select type, count(id) from horae_readytask "
                    " where status = 1 and run_server = '%s' "
                    " group by type;" % public_ip)
            cursor = django.db.connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            len(rows)
            return rows
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
            public_ip = self.__config.get("node", "public_ip")
            if public_ip is None or public_ip.strip() == "":
                public_ip = task_util.StaticFunction.get_local_ip()

            sql = (
                    "select type, owner_id, count(id) from horae_readytask "
                    " where status = 1 and run_server = '%s' "
                    " group by type, owner_id;" %
                    public_ip)
            cursor = django.db.connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            len(rows)
            return rows
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

    def get_owner_mail_list(self, pipeline_id, owner_id):
        tmp_sql = ConstantSql.OWNER_ID_LIST_SQL % pipeline_id
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            id_list = []
            for row in rows:
                id_list.append(int(row[0]))

            mail_list = []
            if len(id_list) > 0:
                perm_historys = horae.models.PermHistory.objects.filter(
                    id__in=id_list,
                    permission=task_util.UserPermissionType.WRITE_STR,
                    status__in=(
                        task_util.AuthAction.CONFIRM_APPLY_AUTH,
                        task_util.AuthAction.GRANT_AUTH_TO_OTHER))
                for perm in perm_historys:
                    if perm.applicant_id is None:
                        continue
                    user = django.contrib.auth.models.User.objects.get(
                        id=perm.applicant_id)
                    if user.email is not None and user.email.strip() != '':
                        mail_list.append(user.email.strip())

            user = django.contrib.auth.models.User.objects.get(
                id=owner_id)
            if user.email is not None and user.email.strip() != '':
                mail_list.append(user.email.strip())

            if len(mail_list) <= 0:
                return None

            return ','.join(mail_list)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex),
                traceback.format_exc()))
            return None
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return None

    @django.db.transaction.atomic
    def stop_task_with_schedule_id(self, schedule_id):
        try:
            with django.db.transaction.atomic():
                schedule = horae.models.Schedule.objects.get(id=schedule_id)
                if schedule.status not in (
                        task_util.TaskState.TASK_READY,
                        task_util.TaskState.TASK_RUNNING,
                        task_util.TaskState.TASK_V100_PENDING,
                        task_util.TaskState.TASK_V100_RUNING,
                        task_util.TaskState.TASK_TIMEOUT,
                        task_util.TaskState.TASK_WAITING):
                    return True

                schedule.status = task_util.TaskState.TASK_STOPED_BY_USER
                run_history = horae.models.RunHistory.objects.get(
                    task_id=schedule.task_id,
                    run_time=schedule.run_time)
                run_history.status = task_util.TaskState.TASK_STOPED_BY_USER
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

    def get_last_rerun(self, schedule_id):
        try:
            reruns = horae.models.RerunHistory.objects.filter(schedule_id=schedule_id).order_by("-id")[: 1]
            if len(reruns) > 0:
                return reruns[0]

            return None
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
                    write_res_id_list.append(str(perm.applicant_id))
                elif perm.permission == UserPermissionType.READ_STR:
                    read_res_id_list.append(str(perm.applicant_id))
                else:
                    pass
            return read_res_id_list, write_res_id_list
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return None, None
        
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

