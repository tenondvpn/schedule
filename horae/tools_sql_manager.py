###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import traceback
import random
import copy

import django.db
import horae.models
import django.db.models
import django.core.exceptions
import django.contrib.auth.models

from horae import models
from horae import tools_util
from common.util import is_admin
from horae import graph_manager


class ConstantSql(object):
    OWNER_ID_LIST_SQL = (
        "select max(id) from horae_permhistory where "
        "resource_type = '%s' and resource_id = %d group by "
        "applicant_id;")

    GET_PACKAGE_HISTORYS = (
        "select a.id, a.upload_time, a.upload_user_id, "
        "b.username, a.version, a.status, a.description, a.update_time, a.name, a.type from ( "
        "    select id, status, upload_time, upload_user_id, "
        "    version, description, update_time, name, type from horae_uploadhistory "
        "    where processor_id = %s  "
        ")a left outer join ( "
        "    select id, username from auth_user "
        ")b on a.upload_user_id = b.id order by a.id desc;")

    GET_PROC_QUOTE_NUM = (
        "select count(id) from horae_task where pid = %s;")

    RESOURCE_ID_LIST_SQL = (
        "select max(id) from horae_permhistory where "
        "resource_type = '%s' and applicant_id = %d group by "
        "resource_id;")

    SHOW_TASK_RUN_HISTORY_SQL = (
        "select task_id, run_time, pl_id, start_time, "
        "end_time, status, schedule_id, pl_name, task_name, id, cpu, mem "
        "from horae_runhistory where pl_id in(%s) %s "
        "order by %s %s limit %s, %s; ")

    SHOW_TASK_RUN_HISTORY_COUNT = (
        "select count(id) as count_his "
        "from horae_runhistory where pl_id in(%s) %s;")

    TASK_SCHEDULED_COUNT_SQL = (
        "select count(a.id) from ("
        "select id, pl_id from horae_runhistory "
        "where status != %s and status != %s and status != %s "
        ") a left outer join("
        "select id from horae_pipeline "
        "where owner_id = %s"
        ") b on a.pl_id = b.id where b.id is not null;")

    SHOW_OWN_PUBLIC_SQL = (
        "select id, name, type, template, update_time, description, "
        "config, owner_id, private, tag, tpl_files, "
        "CASE WHEN quote_num is NULL THEN 0 ELSE quote_num END AS quote_num, project_id from( "
        "   select id, name, type, template, update_time, description, "
        "   config, owner_id, private, tag, tpl_files, project_id from "
        "   horae_processor where  id in(%s) and private = 1 %s "
        ")t1 left outer join ( "
        "   select pid, count(id) as quote_num from horae_task group by pid "
        ")t2 on t1.id=t2.pid "
        "order by %s %s limit %s, %s;")

    PIPE_OWNERS_GET = (
        "select owner_id from horae_pipeline where id in(select "
        "pl_id from horae_task where pid = %s);")

    GET_CT_TIME_BY_TASK_ID_LIST = (
        "select a.id, b.ct_time from( "
        "    select pl_id, id from horae_task where id in(%s) "
        ") a left outer join ( "
        "    select id, ct_time from horae_pipeline "
        ") b on a.pl_id = b.id where b.id is not null;")

    GET_TASK_BY_PIPELINE_ID = (
        "select c.task_id, c.run_time, c.pl_id, c.start_time, "
        "c.end_time, c.status, c.schedule_id, c.pl_name, d.task_name, "
        "d.next_task_ids, d.prev_task_ids, c.cpu, c.mem from( "
        "    select a.task_id, a.run_time, a.pl_id, a.start_time, "
        "    a.end_time, a.status, a.schedule_id, a.cpu, a.mem, "
        "    b.name as pl_name from ( "
        "        select task_id, run_time, pl_id, start_time, "
        "        end_time, status, schedule_id, cpu, mem "
        "        from horae_runhistory where pl_id = %s and run_time = %s "
        "    )a left outer join ( "
        "        select id, name from horae_pipeline "
        "    )b on a.pl_id = b.id "
        ")c left outer join ( "
        "    select id, name as task_name, next_task_ids, prev_task_ids "
        "    from horae_task "
        ")d on c.task_id = d.id ;")

    GET_TASK_INFO_WITH_PIPELINE_NAME = (
        "select a.id, a.pl_id, a.pid, a.next_task_ids, a.prev_task_ids, "
        "a.over_time, a.name, a.retry_count, b.name as pipeline_name from ( "
        "   select id, pl_id, pid, next_task_ids, prev_task_ids, over_time, "
        "   name, retry_count from horae_task where id in(%s) "
        ") a  left outer join ( "
        "   select id, name from horae_pipeline "
        ") b on a.pl_id = b.id;")


class SqlManager(object):
    def __init__(self, logger):
        self.__log = logger
        self.__graph_mgr = graph_manager.GraphGroup(logger)

    def get_all_projects(self, type=0):
        return horae.models.Project.objects.filter(type=type)

    def search_pipeline(self, word, limit):
        return horae.models.Pipeline.objects.filter(name__contains=word)[0: limit]

    def get_projects_with_parent_id(self, parent_id, type=0, owner_id=None):
        if owner_id is None:
            return horae.models.Project.objects.filter(parent_id=parent_id, type=type)

        return horae.models.Project.objects.filter(owner_id=owner_id, parent_id=parent_id, type=type)

    def get_pipelines_with_project_id(self, project_id):
        return horae.models.Pipeline.objects.filter(project_id=project_id)

    def get_pipeline_info(self, pipeline_id):
        try:
            pipeline = horae.models.Pipeline.objects.get(
                id=pipeline_id)
            read_id_list, write_id_list = self.get_owner_id_list(pipeline_id)
            write_id_list.append(pipeline.owner_id)
            owner_users = django.contrib.auth.models.User.objects.filter(
                id__in=write_id_list)
            return owner_users, pipeline
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None, None

        tmp_sql = ConstantSql.OWNER_ID_LIST_SQL % (source_name, source_id)
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            id_list = []
            for row in rows:
                id_list.append(int(row[0]))

            if len(id_list) <= 0:
                return [], []

            perm_historys = models.PermHistory.objects.filter(
                id__in=id_list,
                permission__in=(
                    tools_util.UserPermissionType.WRITE_STR,
                    tools_util.UserPermissionType.READ_STR),
                status__in=(
                    tools_util.AuthAction.CONFIRM_APPLY_AUTH,
                    tools_util.AuthAction.GRANT_AUTH_TO_OTHER))
            read_res_id_list = []
            write_res_id_list = []
            for perm in perm_historys:
                if perm.applicant_id is None:
                    continue

                if perm.permission == tools_util.UserPermissionType.WRITE_STR:
                    write_res_id_list.append(str(perm.applicant_id))
                elif perm.permission == tools_util.UserPermissionType.READ_STR:
                    read_res_id_list.append(str(perm.applicant_id))
                else:
                    pass
            return read_res_id_list, write_res_id_list
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return None, None

    def get_owner_id_list(self, source_id, source_name='pipeline'):
        tmp_sql = ConstantSql.OWNER_ID_LIST_SQL % (source_name, source_id)
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            id_list = []
            for row in rows:
                id_list.append(int(row[0]))

            if len(id_list) <= 0:
                return [], []

            perm_historys = models.PermHistory.objects.filter(
                id__in=id_list,
                permission__in=(
                    tools_util.UserPermissionType.WRITE_STR,
                    tools_util.UserPermissionType.READ_STR),
                status__in=(
                    tools_util.AuthAction.CONFIRM_APPLY_AUTH,
                    tools_util.AuthAction.GRANT_AUTH_TO_OTHER))
            read_res_id_list = []
            write_res_id_list = []
            for perm in perm_historys:
                if perm.applicant_id is None:
                    continue

                if perm.permission == tools_util.UserPermissionType.WRITE_STR:
                    write_res_id_list.append(str(perm.applicant_id))
                elif perm.permission == tools_util.UserPermissionType.READ_STR:
                    read_res_id_list.append(str(perm.applicant_id))
                else:
                    pass
            return read_res_id_list, write_res_id_list
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return None, None

    def get_tasks_by_pipeline_id(self, pipeline_id):
        try:
            tasks = horae.models.Task.objects.filter(
                pl_id=pipeline_id).order_by("id")
            if len(tasks) <= 0:
                return tasks
            return tasks
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def get_proessor_info(self, processor_id):
        try:
            return horae.models.Processor.objects.get(id=processor_id)
        except Exception as ex:
            self.__log.error(
                "execute failed![ex:%s][proc_id:%s][trace:%s]!" % (
                    str(ex), processor_id, traceback.format_exc()))
            return None

    def delete_edge_with_transaction(self, owner_id, from_task_id, to_task_id):
        try:
            with django.db.transaction.atomic():
                return self.delete_edge(owner_id, from_task_id, to_task_id)
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    # def delete_edge(self, owner_id, from_task_id, to_task_id):
    #     from_db_task = horae.models.Task.objects.get(id=from_task_id)
    #     to_db_task = horae.models.Task.objects.get(id=to_task_id)

    #     old_from_task = copy.deepcopy(from_db_task)
    #     from_next_id_list_tmp = from_db_task.next_task_ids.split(',')
    #     from_next_id_list = []
    #     finded = False
    #     for next_id in from_next_id_list_tmp:
    #         if next_id.strip() == '':
    #             continue

    #         if next_id.strip() == str(to_task_id):
    #             finded = True
    #             continue

    #         from_next_id_list.append(next_id.strip())

    #     sql = "update horae_task set next_task_ids='%s' where id=%s and next_task_ids='%s';" % (
    #         ','.join(from_next_id_list), from_db_task.id, from_db_task.next_task_ids)
    #     cursor = django.db.connection.cursor()
    #     change_row = cursor.execute(sql)
    #     if change_row != 1:
    #         raise Exception("exe %s failed, changed"
    #                         " rows[%d]" % (sql, change_row))

    #     old_to_task = copy.deepcopy(to_db_task)
    #     to_prev_id_list_tmp = to_db_task.prev_task_ids.split(',')
    #     to_prev_id_list = []
    #     finded = False
    #     for prev_id in to_prev_id_list_tmp:
    #         if prev_id.strip() == '':
    #             continue

    #         if prev_id.strip() == str(from_task_id):
    #             finded = True
    #             continue
    #         to_prev_id_list.append(prev_id.strip())

    #     sql = "update horae_task set prev_task_ids='%s' where id=%s and prev_task_ids='%s';" % (
    #         ','.join(to_prev_id_list), to_db_task.id, to_db_task.prev_task_ids)
    #     cursor = django.db.connection.cursor()
    #     change_row = cursor.execute(sql)
    #     if change_row != 1:
    #         raise Exception("exe %s failed, changed"
    #                         " rows[%d]" % (sql, change_row))

    #     if not self.__graph_mgr.remove_edge(
    #             str(from_task_id),
    #             str(to_task_id)):
    #         raise Exception("remove edge failed!")

    #     return 0, "OK"

    def delete_edge(self, owner_id, from_task_id, to_task_id):
        if not self.__graph_mgr.remove_edge(
                str(from_task_id),
                str(to_task_id)):
            raise Exception("remove edge failed!")
        try:
            edge = horae.models.Edge.objects.get(prev_task_id=from_task_id, next_task_id=to_task_id)
            edge.delete()
        except Exception as ex:
            self.__log.error("edge not exists: %d, %d!" % (from_task_id, to_task_id))

        return 0, "OK"

    @django.db.transaction.atomic
    def add_edge_with_transaction(self, owner_id, from_task_id, to_task_id, edge):
        try:
            with django.db.transaction.atomic():
                return self.add_edge(owner_id, from_task_id, to_task_id, edge)
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def add_edge(self, owner_id, from_task_id, to_task_id, edge):
        if edge is not None:
            edge.save()

        if not self.__graph_mgr.add_edge(
                str(from_task_id),
                str(to_task_id)):
            raise Exception("add edge failed!")

        return 0, "OK"
    
    # def add_edge(self, owner_id, from_task_id, to_task_id, edge):
    #     if edge is not None:
    #         edge.save()

    #     from_db_task = horae.models.Task.objects.get(id=from_task_id)
    #     next_task_id_list = from_db_task.next_task_ids.split(',')
    #     tmp_next_list = []
    #     for tmp_id in next_task_id_list:
    #         if tmp_id.strip() == '':
    #             continue

    #         tmp_next_list.append(tmp_id.strip())

    #     if str(to_task_id) not in tmp_next_list:
    #         tmp_next_list.append(str(to_task_id))

    #     to_db_task = horae.models.Task.objects.get(id=to_task_id)

    #     prev_task_id_list = to_db_task.prev_task_ids.split(',')
    #     tmp_prev_list = []
    #     for tmp_id in prev_task_id_list:
    #         if tmp_id.strip() == '':
    #             continue

    #         tmp_prev_list.append(tmp_id.strip())

    #     if str(from_task_id) not in tmp_prev_list:
    #         tmp_prev_list.append(str(from_task_id))

    #     old_from_task = copy.deepcopy(from_db_task)
    #     sql = "update horae_task set next_task_ids='%s' where id=%s and next_task_ids='%s';" % (
    #         ','.join(tmp_next_list), from_db_task.id, from_db_task.next_task_ids)
    #     cursor = django.db.connection.cursor()
    #     change_row = cursor.execute(sql)
    #     if change_row != 1:
    #         raise Exception("exe %s failed, changed"
    #                         " rows[%d]" % (sql, change_row))
    #     # from_db_task.next_task_ids = ','.join(tmp_next_list)
    #     # from_db_task.save()
    #     # status, info = self.__update_task_by_check_old_status(
    #     #        from_db_task,
    #     #        old_from_task)
    #     # if status != 0:
    #     #    raise Exception(info)

    #     old_to_task = copy.deepcopy(to_db_task)
    #     sql = "update horae_task set prev_task_ids='%s' where id=%s and prev_task_ids='%s';" % (
    #         ','.join(tmp_prev_list), to_db_task.id, to_db_task.prev_task_ids)
    #     cursor = django.db.connection.cursor()
    #     change_row = cursor.execute(sql)
    #     if change_row != 1:
    #         raise Exception("exe %s failed, changed"
    #                         " rows[%d]" % (sql, change_row))

    #     # to_db_task.prev_task_ids = ','.join(tmp_prev_list)
    #     # to_db_task.save()
    #     # status, info = self.__update_task_by_check_old_status(
    #     #        to_db_task,
    #     #        old_to_task)
    #     # if status != 0:
    #     #    raise Exception(info)
    #     if not self.__graph_mgr.add_edge(
    #             str(from_task_id),
    #             str(to_task_id)):
    #         raise Exception("add edge failed!")
    #     return 0, "OK"

    def get_user_info_by_id(self, user_id):
        try:
            return django.contrib.auth.models.User.objects.get(
                id=user_id)
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    @django.db.transaction.atomic
    def update_pipeline(
            self,
            pipeline_id,
            owner_id,
            lifecycle=None,
            name=None,
            ct_time=None,
            manager_id_list=None,
            monitor_way=None,
            tag=None,
            description=None,
            type=None,
            project_id=None):
        try:
            with django.db.transaction.atomic():
                pipeline = horae.models.Pipeline.objects.get(
                    id=pipeline_id)
                owner_id_valid = True
                if owner_id != pipeline.owner_id:
                    owner_id_valid = False
                read_id_list, write_id_list = self.get_owner_id_list(
                    pipeline_id)

                if self.check_pipeline_auth_valid(
                        pipeline_id,
                        owner_id) != tools_util.UserPermissionType.WRITE:
                    return 1, "对不起，你没有权限修改这个流程!"

                if lifecycle is not None:
                    pipeline.life_cycle = lifecycle

                if name is not None:
                    pipeline.name = name

                if ct_time is not None:
                    pipeline.ct_time = ct_time

                if monitor_way is not None:
                    pipeline.monitor_way = monitor_way

                if tag is not None:
                    pipeline.tag = tag

                if description is not None:
                    pipeline.description = description

                if type is not None:
                    pipeline.type = type

                is_default_project = False
                if pipeline.project_id is not None \
                        and pipeline.project_id != 0 and \
                        (project_id is None or project_id == 0):
                    project = horae.models.Project.objects.get(
                        id=pipeline.project_id, type=0)
                    if project.is_default == 1:
                        project_id = project.id
                        is_default_project = True

                if project_id is not None and project_id != 0:
                    pipeline.project_id = project_id
                else:
                    projects = horae.models.Project.objects.filter(
                        owner_id=owner_id,
                        is_default=1,
                        type=0)
                    if len(projects) <= 0:
                        user_info = django.contrib.auth.models.User.objects.get(
                            id=owner_id)
                        proj_name = "%s_%s" % (
                            user_info.username,
                            tools_util.CONSTANTS.PROJECT_DEFAULT_NAME)
                        project = horae.models.Project(
                            name=proj_name,
                            owner_id=owner_id,
                            is_default=1,
                            type=0)
                        project.save()
                        project_id = project.id
                    else:
                        project_id = projects[0].id

                    pipeline.project_id = project_id

                now_time = tools_util.StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")
                pipeline.update_time = now_time
                if manager_id_list is not None:
                    id_list = manager_id_list.split(',')
                    int_id_list = []
                    for id in id_list:
                        if id.strip() != '':
                            int_id_list.append(int(id))

                    for id in int_id_list:
                        if id == owner_id:
                            continue

                        if str(id) in write_id_list:
                            continue

                        perm_history = models.PermHistory(
                            resource_type=tools_util.CONSTANTS.PIPELINE,
                            resource_id=pipeline_id,
                            permission= \
                                tools_util.UserPermissionType.WRITE_STR,
                            applicant_id=id,
                            grantor_id=owner_id,
                            status= \
                                tools_util.AuthAction.GRANT_AUTH_TO_OTHER,
                            update_time=now_time,
                            create_time=now_time,
                            reason='add manager')
                        perm_history.save()

                    for id in write_id_list:
                        if int(id) == owner_id:
                            continue

                        if int(id) in int_id_list:
                            continue

                        perm_history = models.PermHistory(
                            resource_type=tools_util.CONSTANTS.PIPELINE,
                            resource_id=pipeline_id,
                            permission= \
                                tools_util.UserPermissionType.WRITE_STR,
                            applicant_id=int(id),
                            grantor_id=owner_id,
                            status=tools_util.AuthAction.TAKE_BACK_AUTH,
                            update_time=now_time,
                            reason='take back auth')
                        perm_history.save()
                pipeline.save()

                if ct_time is not None:
                    tasks = horae.models.Task.objects.filter(pl_id=pipeline.id)
                    run_time = tools_util.StaticFunction.get_now_format_time(
                        "%Y%m%d%H%M")
                    for task in tasks:
                        task.last_run_time = run_time
                        task.save()
                return 0, "OK"
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def check_pipeline_auth_valid(self, pipeline_id, owner_id):
        try:
            is_super = is_admin(owner_id)
            if is_super == 1:
                return tools_util.UserPermissionType.WRITE

            pipelines = horae.models.Pipeline.objects.filter(
                id=pipeline_id,
                owner_id=owner_id)
            if len(pipelines) > 0:
                return tools_util.UserPermissionType.WRITE

            owners = models.PermHistory.objects.filter(
                resource_type=tools_util.CONSTANTS.PIPELINE,
                resource_id=pipeline_id,
                applicant_id=owner_id).order_by('-id')[: 1]
            if len(owners) <= 0:
                return tools_util.UserPermissionType.NO_AUTH

            if owners[0].status not in (
                    tools_util.AuthAction.CONFIRM_APPLY_AUTH,
                    tools_util.AuthAction.GRANT_AUTH_TO_OTHER):
                return tools_util.UserPermissionType.NO_AUTH

            if owners[0].permission == tools_util.UserPermissionType.READ_STR:
                return tools_util.UserPermissionType.READ

            if owners[0].permission == tools_util.UserPermissionType.WRITE_STR:
                return tools_util.UserPermissionType.WRITE

            return tools_util.UserPermissionType.NO_AUTH
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return tools_util.UserPermissionType.NO_AUTH

    # @django.db.transaction.atomic
    # def copy_pipeline(self, owner_id, src_pl_id, new_pl_name, project_id, use_type_src):
    #     try:
    #         with django.db.transaction.atomic():
    #             new_pl_name = new_pl_name.strip()
    #             new_pl_name = new_pl_name.replace('\r', '')
    #             new_pl_name = new_pl_name.replace('\n', '')
    #             pipeline = horae.models.Pipeline.objects.get(id=src_pl_id)
    #             if pipeline.name == new_pl_name:
    #                 return 1, "流程名重名"

    #             pipe_type = pipeline.type
    #             if not use_type_src:
    #                 pipe_type = 0

    #             status, info = self.create_new_pipeline(
    #                 name=new_pl_name,
    #                 ct_time=pipeline.ct_time,
    #                 owner_id=owner_id,
    #                 manager_id_list='',
    #                 monitor_way=pipeline.monitor_way,
    #                 tag=pipeline.tag,
    #                 description=pipeline.description,
    #                 life_cycle=pipeline.life_cycle,
    #                 type=pipe_type,
    #                 project_id=project_id)
    #             if status != 0:
    #                 raise Exception(
    #                     "copy new pipeline failed![%s]" % info)

    #             new_pipeline = horae.models.Pipeline.objects.get(
    #                 name=new_pl_name)
    #             src_tasks = horae.models.Task.objects.filter(pl_id=src_pl_id)
    #             old_task_map = {}
    #             new_task_map = {}
    #             old_new_task_id_map = {}
    #             old_task_id_set = set()
    #             for src_task in src_tasks:
    #                 new_task = horae.models.Task(
    #                     pl_id=new_pipeline.id,
    #                     pid=src_task.pid,
    #                     next_task_ids='',
    #                     prev_task_ids='',
    #                     over_time=src_task.over_time,
    #                     name=src_task.name,
    #                     config=src_task.config,
    #                     retry_count=src_task.retry_count,
    #                     last_run_time=src_task.last_run_time,
    #                     description=src_task.description,
    #                     priority=src_task.priority,
    #                     except_ret=src_task.except_ret,
    #                     server_tag=src_task.server_tag,
    #                     version_id=src_task.version_id)
    #                 status, add_task = self.add_new_task_to_pipeline(
    #                     owner_id,
    #                     new_task,
    #                     None)
    #                 if status != 0:
    #                     self.delete_pipeline(owner_id, new_pipeline.id)
    #                     raise Exception(add_task)
    #                 new_task = horae.models.Task.objects.get(
    #                     pl_id=new_pipeline.id,
    #                     name=src_task.name)
    #                 old_task_map[new_task.id] = src_task
    #                 new_task_map[new_task.id] = new_task
    #                 old_task_id_set.add(src_task.id)
    #                 old_new_task_id_map[src_task.id] = new_task.id

    #             for task_id in new_task_map:
    #                 prev_id_str = ''
    #                 if old_task_map[task_id].prev_task_ids is not None \
    #                         and old_task_map[task_id].prev_task_ids != '':
    #                     prev_id_list = []
    #                     old_prev_task_id_list = \
    #                         old_task_map[task_id].prev_task_ids.split(',')
    #                     for old_prev_id in old_prev_task_id_list:
    #                         if old_prev_id.strip() == "":
    #                             continue

    #                         if int(old_prev_id) not in old_task_id_set:
    #                             continue
    #                         prev_id_list.append(
    #                             str(old_new_task_id_map[int(old_prev_id)]))

    #                     prev_id_str = ",".join(prev_id_list)

    #                 next_id_str = ''
    #                 if old_task_map[task_id].next_task_ids is not None \
    #                         and old_task_map[task_id].next_task_ids != '':
    #                     next_id_list = []
    #                     old_next_task_id_list = \
    #                         old_task_map[task_id].next_task_ids.split(',')
    #                     for old_next_id in old_next_task_id_list:
    #                         if old_next_id.strip() == "":
    #                             continue

    #                         if int(old_next_id) not in old_task_id_set:
    #                             continue
    #                         next_id_list.append(
    #                             str(old_new_task_id_map[int(old_next_id)]))
    #                     next_id_str = ",".join(next_id_list)
    #                 new_task_map[task_id].prev_task_ids = prev_id_str
    #                 new_task_map[task_id].next_task_ids = next_id_str
    #                 new_task_map[task_id].save()
    #             return 0, new_pipeline.id
    #     except Exception as ex:
    #         self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
    #             str(ex), traceback.format_exc()))
    #         return 1, str(ex)

    @django.db.transaction.atomic
    def copy_pipeline(self, owner_id, src_pl_id, new_pl_name, project_id, use_type_src):
        try:
            with django.db.transaction.atomic():
                new_pl_name = new_pl_name.strip()
                new_pl_name = new_pl_name.replace('\r', '')
                new_pl_name = new_pl_name.replace('\n', '')
                pipeline = horae.models.Pipeline.objects.get(id=src_pl_id)
                if pipeline.name == new_pl_name:
                    return 1, "DAG流名重名"

                pipe_type = pipeline.type
                if not use_type_src:
                    pipe_type = 0

                status, info = self.create_new_pipeline(
                    name=new_pl_name,
                    ct_time=pipeline.ct_time,
                    owner_id=owner_id,
                    manager_id_list='',
                    monitor_way=pipeline.monitor_way,
                    tag=pipeline.tag,
                    description=pipeline.description,
                    life_cycle=pipeline.life_cycle,
                    type=pipe_type,
                    project_id=project_id)
                if status != 0:
                    raise Exception(
                        "copy new pipeline failed![%s]" % info)

                new_pipeline = horae.models.Pipeline.objects.get(
                    name=new_pl_name)
                src_tasks = horae.models.Task.objects.filter(pl_id=src_pl_id)
                old_new_task_id_map = {}
                old_task_id_set = set()
                for src_task in src_tasks:
                    new_task = horae.models.Task(
                        pl_id=new_pipeline.id,
                        pid=src_task.pid,
                        next_task_ids='',
                        prev_task_ids='',
                        over_time=src_task.over_time,
                        name=src_task.name,
                        config=src_task.config,
                        retry_count=src_task.retry_count,
                        last_run_time=src_task.last_run_time,
                        description=src_task.description,
                        priority=src_task.priority,
                        except_ret=src_task.except_ret,
                        server_tag=src_task.server_tag,
                        version_id=src_task.version_id)
                    status, add_task = self.add_new_task_to_pipeline(
                        owner_id,
                        new_task,
                        None)
                    if status != 0:
                        self.delete_pipeline(owner_id, new_pipeline.id)
                        raise Exception(add_task)
                    old_new_task_id_map[src_task.id] = new_task.id

                old_edges = horae.models.Edge.objects.filter(pipeline_id=src_pl_id)
                for edge in old_edges:
                    new_prev_task_id = old_new_task_id_map[edge.prev_task_id]
                    new_next_task_id = old_new_task_id_map[edge.next_task_id]
                    new_edge = horae.models.Edge(
                        prev_task_id=int(new_prev_task_id),
                        next_task_id=int(new_next_task_id),
                        stream_type=int(edge.stream_type),
                        file_name=edge.file_name,
                        rcm_context=edge.rcm_context,
                        rcm_topic=edge.rcm_topic,
                        rcm_partition=int(edge.rcm_partition),
                        dispatch_tag=int(edge.dispatch_tag),
                        pipeline_id=new_pipeline.id
                    )
                    self.add_edge(owner_id, new_prev_task_id, new_next_task_id, new_edge)

                return 0, new_pipeline.id
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)
        
    @django.db.transaction.atomic
    def create_new_pipeline(
            self,
            name,
            ct_time,
            owner_id,
            manager_id_list,
            monitor_way,
            tag,
            description,
            life_cycle,
            type,
            project_id):
        try:
            with django.db.transaction.atomic():
                name = name.strip()
                if project_id is None or project_id == 0:
                    projects = horae.models.Project.objects.filter(
                        owner_id=owner_id, is_default=1, type=0)
                    if len(projects) <= 0:
                        user_info = django.contrib.auth.models.User.objects.get(
                            id=owner_id)
                        proj_name = "%s_%s" % (
                            user_info.username,
                            tools_util.CONSTANTS.PROJECT_DEFAULT_NAME)
                        project = horae.models.Project(
                            name=proj_name,
                            owner_id=owner_id,
                            is_default=1,
                            type=0)
                        project.save()
                        project_id = project.id
                    else:
                        project_id = projects[0].id

                now_time = tools_util.StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")
                pipeline = horae.models.Pipeline(
                    name=name,
                    owner_id=owner_id,
                    ct_time=ct_time,
                    life_cycle=life_cycle,
                    update_time=now_time,
                    email_to="",
                    description=description,
                    sms_to="",
                    monitor_way=monitor_way,
                    enable=0,
                    tag=tag,
                    private=1,
                    type=type,
                    project_id=project_id)
                pipeline.save()
                if manager_id_list is None:
                    return 0, "OK"

                pipeline = horae.models.Pipeline.objects.get(name=name)
                id_list = manager_id_list.split(",")
                for id in id_list:
                    if id.strip() == '':
                        continue

                    if int(id) == owner_id:
                        continue

                    print(id, int(id), owner_id)
                    perm_history = models.PermHistory(
                        resource_type=tools_util.CONSTANTS.PIPELINE,
                        resource_id=pipeline.id,
                        permission=tools_util.UserPermissionType.WRITE_STR,
                        applicant_id=int(id),
                        grantor_id=owner_id,
                        status=tools_util.AuthAction.GRANT_AUTH_TO_OTHER,
                        update_time=now_time,
                        create_time=now_time,
                        reason='add manager')
                    perm_history.save()
                return 0, "OK"
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def add_new_task_to_pipeline(self, owner_id, task, processor):
        add_edge_list = []
        try:
            with django.db.transaction.atomic():
                if processor is not None:
                    processor.name = "%s_%s%s" % (
                        processor.name,
                        task.pl_id,
                        random.randint(1, 100))
                    processor.save()
                    processor = horae.models.Processor.objects.get(
                        name=processor.name)
                    task.pid = processor.id

                    upload_his = horae.models.UploadHistory(
                        processor_id=processor.id,
                        status=0,
                        update_time=tools_util.StaticFunction.get_now_format_time("%Y-%m-%d %H:%M:%S"),
                        upload_time=tools_util.StaticFunction.get_now_format_time("%Y-%m-%d %H:%M:%S"),
                        upload_user_id=owner_id,
                        version="0.0.0",
                        name="0.0.0",
                        description="auto create",
                        git_url="")
                    upload_his.save()

                # 检查算子是否存在
                processor = horae.models.Processor.objects.get(
                    id=task.pid)
                pipeline = horae.models.Pipeline.objects.get(id=task.pl_id)
                if self.check_pipeline_auth_valid(
                        pipeline.id,
                        owner_id) != tools_util.UserPermissionType.WRITE:
                    return 1, "对不起，你没有权限向这个流程添加任务！"

                task.last_run_time = \
                    tools_util.StaticFunction.get_now_format_time(
                        "%Y%m%d%H%M")

                if task.prev_task_ids is None:
                    task.prev_task_ids = ','

                if task.next_task_ids is None:
                    task.next_task_ids = ','

                prev_task_ids = copy.deepcopy(task.prev_task_ids)
                next_task_ids = copy.deepcopy(task.next_task_ids)
                task.prev_task_ids = ','
                task.next_task_ids = ','
                task.save()
                new_task = horae.models.Task.objects.get(
                    name=task.name,
                    pl_id=task.pl_id)
                if next_task_ids is not None:
                    next_task_id_list = next_task_ids.split(",")
                    for next_id in next_task_id_list:
                        if next_id.strip() == '':
                            continue
                        self.add_edge(owner_id, new_task.id, int(next_id.strip()))
                        add_edge_list.append((new_task.id, int(next_id.strip())))

                if prev_task_ids is not None:
                    prev_task_id_list = prev_task_ids.split(",")
                    for prev_id in prev_task_id_list:
                        if prev_id.strip() == '':
                            continue
                        self.add_edge(owner_id, int(prev_id.strip()), new_task.id)
                        add_edge_list.append((int(prev_id.strip()), new_task.id))

                self.__graph_mgr.add_node(str(new_task.id))
                new_task = horae.models.Task.objects.get(
                    name=task.name,
                    pl_id=task.pl_id)
                return 0, new_task
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            for add_edge in add_edge_list:
                self.__graph_mgr.remove_edge(add_edge[0], add_edge[1])
            return 1, str(ex)

    def delete_pipeline(self, owner_id, pipeline_id):
        try:
            with django.db.transaction.atomic():
                pipeline = horae.models.Pipeline.objects.get(
                    id=pipeline_id)
                if self.check_pipeline_auth_valid(
                        pipeline_id,
                        owner_id) != tools_util.UserPermissionType.WRITE:
                    return 1, "对不起，你没有权限删除这个DAG流!"

                run_tasks = horae.models.Schedule.objects.filter(
                    pl_id=pipeline_id,
                    status__in=(
                        tools_util.TaskState.TASK_READY,
                        tools_util.TaskState.TASK_RUNNING,
                        tools_util.TaskState.TASK_WAITING))
                if len(run_tasks) > 0:
                    return 1, "can't delete pipeline, there has task running!"

                tasks = horae.models.Task.objects.filter(
                    pl_id=pipeline_id)
                for task in tasks:
                    self.__graph_mgr.remove_node(task.id)
                tasks.delete()
                edges = horae.models.Edge.objects.filter(
                    pipeline_id=pipeline_id)
                edges.delete()
                pipeline.delete()
                perm_historys = models.PermHistory.objects.filter(
                    resource_type=tools_util.CONSTANTS.PIPELINE,
                    resource_id=pipeline_id)
                for perm_his in perm_historys:
                    perm_his.delete()

                run_histories = horae.models.RunHistory.objects.filter(
                    pl_id=pipeline_id)
                for run_history in run_histories:
                    run_history.delete()

                schedules = horae.models.Schedule.objects.filter(
                    pl_id=pipeline_id)
                for schedule in schedules:
                    schedule.delete()

                ready_tasks = horae.models.ReadyTask.objects.filter(
                    pl_id=pipeline_id)
                for ready_task in ready_tasks:
                    ready_task.delete()

                ordered_tasks = horae.models.OrderdSchedule.objects.filter(
                    pl_id=pipeline_id)
                for ordered_task in ordered_tasks:
                    ordered_task.delete()
                return 0, "OK"
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)
        
    # def delete_pipeline(self, owner_id, pipeline_id):
    #     try:
    #         with django.db.transaction.atomic():
    #             pipeline = horae.models.Pipeline.objects.get(
    #                 id=pipeline_id)
    #             if self.check_pipeline_auth_valid(
    #                     pipeline_id,
    #                     owner_id) != tools_util.UserPermissionType.WRITE:
    #                 return 1, "对不起，你没有权限删除这个流程!"

    #             run_tasks = horae.models.Schedule.objects.filter(
    #                 pl_id=pipeline_id,
    #                 status__in=(
    #                     tools_util.TaskState.TASK_READY,
    #                     tools_util.TaskState.TASK_RUNNING,
    #                     tools_util.TaskState.TASK_WAITING))
    #             if len(run_tasks) > 0:
    #                 return 1, "can't delete pipeline, there has task running!"

    #             tasks = horae.models.Task.objects.filter(
    #                 pl_id=pipeline_id)
    #             for task in tasks:
    #                 next_task_ids = task.next_task_ids.split(',')
    #                 other_dep_tasks = []
    #                 for tmp_task_id in next_task_ids:
    #                     if tmp_task_id.strip() == '':
    #                         continue

    #                     tmp_tasks = horae.models.Task.objects.filter(id=int(tmp_task_id))
    #                     if len(tmp_tasks) <= 0:
    #                         continue

    #                     tmp_task = horae.models.Task.objects.get(id=int(tmp_task_id))
    #                     if tmp_task.pl_id != task.pl_id:
    #                         pipeline = horae.models.Pipeline.objects.get(id=tmp_task.pl_id)
    #                         other_dep_tasks.append("流程：%s\t任务：%s" % (pipeline.name, tmp_task.name))
    #                 if len(other_dep_tasks) > 0:
    #                     return 2, ("这个流程的任务被其他流程的任务依赖,"
    #                                "请先解除依赖关系再删除。\n%s" %
    #                                '\n'.join(other_dep_tasks))

    #             delete_task_id_list = []
    #             for task in tasks:
    #                 delete_task_id_list.append(str(task.id))
    #                 task.delete()
    #             pipeline.delete()

    #             for remove_node in delete_task_id_list:
    #                 self.__graph_mgr.remove_node(remove_node)
    #             perm_historys = models.PermHistory.objects.filter(
    #                 resource_type=tools_util.CONSTANTS.PIPELINE,
    #                 resource_id=pipeline_id)
    #             for perm_his in perm_historys:
    #                 perm_his.delete()

    #             run_histories = horae.models.RunHistory.objects.filter(
    #                 pl_id=pipeline_id)
    #             for run_history in run_histories:
    #                 run_history.delete()

    #             schedules = horae.models.Schedule.objects.filter(
    #                 pl_id=pipeline_id)
    #             for schedule in schedules:
    #                 schedule.delete()

    #             ready_tasks = horae.models.ReadyTask.objects.filter(
    #                 pl_id=pipeline_id)
    #             for ready_task in ready_tasks:
    #                 ready_task.delete()

    #             ordered_tasks = horae.models.OrderdSchedule.objects.filter(
    #                 pl_id=pipeline_id)
    #             for ordered_task in ordered_tasks:
    #                 ordered_task.delete()
    #             return 0, "OK"
    #     except Exception as ex:
    #         self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
    #             str(ex), traceback.format_exc()))
    #         return 1, str(ex)

    def update_tasks(self, owner_id, task, old_task, template):
        added_edges = []
        removed_edges = []
        try:
            with django.db.transaction.atomic():
                db_task = horae.models.Task.objects.get(id=task.id)
                task.pl_id = db_task.pl_id
                old_next_ids = db_task.next_task_ids
                old_prev_ids = db_task.prev_task_ids

                if self.check_pipeline_auth_valid(
                        task.pl_id,
                        owner_id) != tools_util.UserPermissionType.WRITE:
                    return 1, "对不起，你没有权限修改这个流程的任务！"

                new_next_task_ids = task.next_task_ids
                new_prev_task_ids = task.prev_task_ids
                if task.next_task_ids == 'null':
                    task.next_task_ids = ','

                if task.prev_task_ids == 'null':
                    task.prev_task_ids = ','
                if new_next_task_ids is not None:
                    new_id_list = new_next_task_ids.split(',')
                    new_id_strip_list = []
                    for id in new_id_list:
                        if id.strip() == '' or id.strip() == 'null':
                            continue
                        try:
                            int(id)
                        except:
                            continue
                        new_id_strip_list.append(id.strip())
                    old_id_list = old_next_ids.split(',')
                    old_id_strip_list = []
                    for id in old_id_list:
                        if id.strip() == '' or id.strip() == 'null':
                            continue
                        old_id_strip_list.append(id.strip())
                        if id.strip() not in new_id_strip_list:
                            self.delete_edge(owner_id, task.id, int(id.strip()))
                            self.__graph_mgr.remove_edge(
                                str(task.id),
                                id.strip())
                            removed_edges.append((str(task.id), id.strip()))
                    for id in new_id_strip_list:
                        if id not in old_id_strip_list:
                            self.add_edge(owner_id, task.id, int(id.strip()))
                            if not self.__graph_mgr.add_edge(str(task.id), id):
                                raise Exception("add edge failed!")
                            added_edges.append((str(task.id), id))
                    task.next_task_ids = ','.join(new_id_strip_list)

                if new_prev_task_ids is not None:
                    new_id_list = new_prev_task_ids.split(',')
                    new_id_strip_list = []
                    for id in new_id_list:
                        if id.strip() == '' or id.strip() == 'null':
                            continue
                        try:
                            int(id)
                        except:
                            continue

                        new_id_strip_list.append(id.strip())
                    old_id_list = old_prev_ids.split(',')
                    old_id_strip_list = []
                    for id in old_id_list:
                        if id.strip() == '' or id.strip() == 'null':
                            continue
                        old_id_strip_list.append(id.strip())
                        if id.strip() not in new_id_strip_list:
                            self.delete_edge(owner_id, int(id.strip()), task.id)
                            self.__graph_mgr.remove_edge(
                                id.strip(),
                                str(task.id))
                            removed_edges.append((id.strip(), str(task.id)))

                    for id in new_id_strip_list:
                        if id not in old_id_strip_list:
                            self.add_edge(owner_id, int(id.strip()), task.id)
                            if not self.__graph_mgr.add_edge(id, str(task.id)):
                                raise Exception("add edge failed!")
                            added_edges.append((id, str(task.id)))
                    task.prev_task_ids = ','.join(new_id_strip_list)
                status, info = 0, "OK"
                # if old_task is not None:
                #    status, info = self.__update_task_by_check_old_status(
                #            task,
                #            old_task)
                # else:

                if template is not None:
                    task_count = horae.models.Task.objects.filter(pid=task.pid).count()
                    if task_count == 1:
                        proc = horae.models.Processor.objects.get(id=task.pid)
                        proc.template = template
                        proc.save()
                task.save()
                return status, task
        except Exception as ex:
            for edge in added_edges:
                self.__graph_mgr.remove_edge(edge[0], edge[1])

            for edge in removed_edges:
                self.__graph_mgr.add_edge(edge[0], edge[1])

            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def get_task_info(self, task_id):
        try:
            return horae.models.Task.objects.get(id=task_id)
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def get_processor_package_history(self, processor_id):
        tmp_sql = ConstantSql.GET_PACKAGE_HISTORYS % (processor_id)
        print("get upload history: " + tmp_sql)
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            return rows
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def get_processor_quote_num(self, processor_id):
        tmp_sql = ConstantSql.GET_PROC_QUOTE_NUM % processor_id
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            return rows[0][0]
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return 1

    def get_all_authed_pipeline_info(self, owner_id, task_id=None):
        try:
            pipelines = horae.models.Pipeline.objects.filter(
                type=0).values('id', 'name', 'type')
            len(pipelines)
            return pipelines
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return []

    def get_processor_with_project_id(self, project_id, owner_id=None):
        if owner_id is None:
            return horae.models.Processor.objects.filter(project_id=project_id)

        return horae.models.Processor.objects.filter(project_id=project_id, owner_id=owner_id)

    def get_all_authed_processor(self, owner_id, type):
        try:
            processors_pub = horae.models.Processor.objects.exclude(
                owner_id=owner_id).filter(
                private=0,
                type=type)
            processors_own = horae.models.Processor.objects.filter(
                owner_id=owner_id,
                type=type)
            read_list, write_list = self.get_pipeline_id_list_by_owner_id(
                owner_id,
                tools_util.CONSTANTS.PROCESSOR)
            processors_other = horae.models.Processor.objects.filter(
                id__in=read_list)
            return processors_pub | processors_own | processors_other
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def get_pipeline_id_list_by_owner_id(self, owner_id, source_name='pipeline'):
        """
            获取每一个owener_id所有最后修改的权限信息相关的流程信息
            并返回有读写权限的流程
            (不包含自己创建的）
        """
        tmp_sql = ConstantSql.RESOURCE_ID_LIST_SQL % (source_name, owner_id)
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            id_list = []
            for row in rows:
                id_list.append(int(row[0]))
            if len(id_list) <= 0:
                return [], []

            perm_historys = models.PermHistory.objects.filter(
                id__in=id_list,
                permission__in=(
                    tools_util.UserPermissionType.WRITE_STR,
                    tools_util.UserPermissionType.READ_STR),
                status__in=(
                    tools_util.AuthAction.CONFIRM_APPLY_AUTH,
                    tools_util.AuthAction.GRANT_AUTH_TO_OTHER))
            read_res_id_list = []
            write_res_id_list = []
            for perm in perm_historys:
                if perm.permission == tools_util.UserPermissionType.WRITE_STR:
                    write_res_id_list.append(str(perm.resource_id))
                elif perm.permission == tools_util.UserPermissionType.READ_STR:
                    read_res_id_list.append(str(perm.resource_id))
                else:
                    pass
            return read_res_id_list, write_res_id_list
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return [], []

    def get_all_user_info(self):
        try:
            users = django.contrib.auth.models.User.objects.all()
            if len(users) <= 0:  # 防止惰性计算将异常传到外部
                return users
            return users
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def search_processor(self, word, limit):
        return horae.models.Processor.objects.filter(name__contains=word)[0: limit]

    def get_processor_quote_list(self, processor_id):
        try:
            quote_list = []
            tasks = horae.models.Task.objects.filter(pid=processor_id)[0: 100]
            for task in tasks:
                quote_map = {}
                quote_map["task_id"] = task.id
                quote_map["task_name"] = task.name
                pipelines = horae.models.Pipeline.objects.filter(id=task.pl_id)
                if len(pipelines) <= 0:
                    continue
                pipeline = pipelines[0]
                quote_map["pipeline_id"] = task.pl_id
                quote_map["pipeline_name"] = pipeline.name
                owner_users = django.contrib.auth.models.User.objects.filter(
                    id=pipeline.owner_id)
                if len(owner_users) <= 0:
                    self.__log.warn(
                        "pipeline owner id not exits:pl_id: %d, owner_id: %d" %
                        (pipeline.id, pipeline.owner_id))
                    continue
                owner_users = owner_users[0]
                quote_map["owner_id"] = owner_users.id
                quote_map["owner_name"] = owner_users.username
                quote_map["last_run_time"] = task.last_run_time
                quote_list.append(quote_map)
            return quote_list
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    @django.db.transaction.atomic
    def update_processor(self, user_id, processor, user_id_list):
        is_super = is_admin(user_id)
        try:
            with django.db.transaction.atomic():
                old_processor = horae.models.Processor.objects.get(
                    id=processor.id)

                if old_processor.owner_id != user_id:
                    if is_super != 1:
                        return 1, "必须是算子的owner才能修改算子信息！"

                while processor.config is not None:
                    old_key_list = self.__get_config_key_list(old_processor.config)
                    new_key_list = self.__get_config_key_list(processor.config)
                    append_key_list = []
                    for new_key in new_key_list:
                        if new_key not in old_key_list:
                            append_key_list.append(new_key)

                    if len(append_key_list) <= 0:
                        break

                    tasks = horae.models.Task.objects.filter(pid=processor.id)
                    if len(tasks) <= 0:
                        break

                    for task in tasks:
                        append_list = []
                        task_key_list = self.__get_config_key_list(task.config)
                        for append_key in append_key_list:
                            if append_key not in task_key_list:
                                tmp_str = "%s=" % append_key
                                append_list.append(tmp_str)
                        if len(append_list) <= 0:
                            continue
                        append_str = '\n'.join(append_list)
                        task_config = "%s\n%s" % (task.config, append_str)
                        task.config = task_config
                        task.save()
                    break  # break for while
                processor.save()

                now_time = tools_util.StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")

                read_id_list, write_id_list = self.get_owner_id_list(
                    processor.id,
                    tools_util.CONSTANTS.PROCESSOR)
                if user_id_list is not None:
                    id_list = user_id_list.split(',')
                    int_id_list = []
                    for id in id_list:
                        if id.strip() != '':
                            int_id_list.append(int(id))

                    for id in int_id_list:
                        if id == user_id:
                            continue

                        if str(id) in read_id_list:
                            continue

                        perm_history = models.PermHistory(
                            resource_type=tools_util.CONSTANTS.PROCESSOR,
                            resource_id=processor.id,
                            permission= \
                                tools_util.UserPermissionType.READ_STR,
                            applicant_id=id,
                            grantor_id=user_id,
                            status= \
                                tools_util.AuthAction.GRANT_AUTH_TO_OTHER,
                            update_time=now_time,
                            create_time=now_time,
                            reason='add manager')
                        perm_history.save()

                    for id in read_id_list:
                        if int(id) == user_id:
                            continue

                        if int(id) in int_id_list:
                            continue

                        perm_history = models.PermHistory(
                            resource_type=tools_util.CONSTANTS.PROCESSOR,
                            resource_id=processor.id,
                            permission= \
                                tools_util.UserPermissionType.READ_STR,
                            applicant_id=int(id),
                            grantor_id=user_id,
                            status=tools_util.AuthAction.TAKE_BACK_AUTH,
                            update_time=now_time,
                            reason='take back auth')
                        perm_history.save()
                return 0, "OK"
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def __get_config_key_list(self, config):
        key_list = []
        if config is None:
            return key_list

        config_list = config.split('\n')
        for config_item in config_list:
            if config_item.strip() == '':
                continue

            item_kv = config_item.split('=')
            if len(item_kv) < 2:
                continue

            if item_kv[0].strip() == '':
                continue

            key_list.append(item_kv[0].strip())

        return key_list

    def show_task_history(
            self,
            owner_id,
            page_min,
            page_max,
            order_field,
            sort_order,
            where_content):
        try:
            tmp_list = []
            cursor = django.db.connection.cursor()
            is_super = is_admin(owner_id)
            pipelines = horae.models.Pipeline.objects.all().values("id")
            for pipeline in pipelines:
                tmp_list.append(str(pipeline["id"]))
            '''
            if is_super == 1:
                pipelines = horae.models.Pipeline.objects.all().values("id")
                for pipeline in pipelines:
                    tmp_list.append(str(pipeline["id"]))
            else:
                sql = ("select distinct id from horae_pipeline"
                        " where owner_id=%s;" % owner_id)
                cursor.execute(sql) 
                rows = cursor.fetchall()
                owner_id_list = []
                for row in rows:
                    owner_id_list.append(str(row[0]))
                tmp_list, write_res_id_list = \
                        self.get_pipeline_id_list_by_owner_id(owner_id)
                tmp_list.extend(write_res_id_list)
                tmp_list.extend(owner_id_list)
           '''
            if len(tmp_list) <= 0:
                return 0, []

            pipeline_id_list = ','.join(tmp_list)
            page_max = page_max - page_min
            sql = ConstantSql.SHOW_TASK_RUN_HISTORY_SQL % (
                pipeline_id_list,
                where_content,
                order_field,
                sort_order,
                page_min,
                page_max)
            cursor.execute(sql)
            rows = cursor.fetchall()
            sql = ConstantSql.SHOW_TASK_RUN_HISTORY_COUNT % (
                pipeline_id_list,
                where_content)
            cursor.execute(sql)
            print(sql)
            count = cursor.fetchall()
            return count[0][0], rows
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 0, None

    def get_run_history_info(self, run_history_id):
        try:
            return horae.models.RunHistory.objects.get(id=run_history_id)
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def get_now_owner_scheduled_task_num(self, owner_id):
        try:
            sql = ConstantSql.TASK_SCHEDULED_COUNT_SQL % (
                tools_util.TaskState.TASK_FAILED,
                tools_util.TaskState.TASK_SUCCEED,
                tools_util.TaskState.TASK_STOPED_BY_USER,
                owner_id)
            cursor = django.db.connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return True, rows[0][0]
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex),
                traceback.format_exc()))
            return False, 0
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return False, 0

    def create_processor(self, processor, user_id_list):
        try:
            with django.db.transaction.atomic():
                processor.save()
                now_time = tools_util.StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")
                read_id_list, write_id_list = self.get_owner_id_list(
                    processor.id,
                    tools_util.CONSTANTS.PROCESSOR)
                if user_id_list is not None:
                    id_list = user_id_list.split(',')
                    int_id_list = []
                    for id in id_list:
                        if id.strip() != '':
                            int_id_list.append(int(id))

                    for id in int_id_list:
                        if id == processor.owner_id:
                            continue

                        if str(id) in read_id_list:
                            continue

                        perm_history = models.PermHistory(
                            resource_type=tools_util.CONSTANTS.PROCESSOR,
                            resource_id=processor.id,
                            permission= \
                                tools_util.UserPermissionType.READ_STR,
                            applicant_id=id,
                            grantor_id=processor.owner_id,
                            status= \
                                tools_util.AuthAction.GRANT_AUTH_TO_OTHER,
                            update_time=now_time,
                            create_time=now_time,
                            reason='add manager')
                        perm_history.save()

                    for id in read_id_list:
                        if int(id) == processor.owner_id:
                            continue

                        if int(id) in int_id_list:
                            continue

                        perm_history = models.PermHistory(
                            resource_type=tools_util.CONSTANTS.PROCESSOR,
                            resource_id=processor.id,
                            permission= \
                                tools_util.UserPermissionType.READ_STR,
                            applicant_id=int(id),
                            grantor_id=processor.owner_id,
                            status=tools_util.AuthAction.TAKE_BACK_AUTH,
                            update_time=now_time,
                            reason='take back auth')
                        perm_history.save()
            return 0, "OK"
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def show_processor_own_public(
            self,
            owner_id,
            page_min,
            page_max,
            order_field,
            sort_order,
            where_content):
        read_list, write_list = self.get_pipeline_id_list_by_owner_id(
            owner_id,
            tools_util.CONSTANTS.PROCESSOR)
        if len(read_list) <= 0:
            return []

        read_id_str = ','.join(read_list)
        page_max = page_max - page_min
        tmp_sql = ConstantSql.SHOW_OWN_PUBLIC_SQL % (
            read_id_str,
            where_content,
            order_field,
            sort_order,
            page_min,
            page_max)
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            own_pub_rows = cursor.fetchall()
            return own_pub_rows
        except Exception as ex:
            self.__log.error("execute sql[%s] failed![ex:%s][trace:%s]!" % (
                tmp_sql, str(ex), traceback.format_exc()))
            return None

    def add_new_project(self, owner_id, project_name, writer_list, description, parent_id, type=0):
        try:
            with django.db.transaction.atomic():
                if project_name.find(
                        tools_util.CONSTANTS.PROJECT_DEFAULT_NAME) >= 0:
                    return 1, ("项目名中不能包含 '%s' 关键词！" %
                               tools_util.CONSTANTS.PROJECT_DEFAULT_NAME), 0

                project = horae.models.Project(
                    name=project_name,
                    owner_id=owner_id,
                    description=description,
                    is_default=False,
                    parent_id=parent_id,
                    type=type)

                project.save()
                if writer_list is None or writer_list.strip() == '':
                    return 0, "OK", project.id

                project = horae.models.Project.objects.get(name=project_name, type=0)
                id_list = writer_list.split(",")
                now_time = tools_util.StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")
                for id in id_list:
                    if id.strip() == '':
                        continue

                    if int(id) == owner_id:
                        continue

                    perm_history = models.PermHistory(
                        resource_type=tools_util.CONSTANTS.PROJECT,
                        resource_id=project.id,
                        permission=tools_util.UserPermissionType.WRITE_STR,
                        applicant_id=int(id),
                        grantor_id=owner_id,
                        status=tools_util.AuthAction.GRANT_AUTH_TO_OTHER,
                        update_time=now_time,
                        create_time=now_time,
                        reason='add manager')
                    perm_history.save()
                return 0, "OK", project.id
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex), 0

    def public_processor(self, processor_id, project_id):
        try:
            processor = horae.models.Processor.objects.get(id=processor_id)
            if processor.private == 0:
                tmp_sql = ConstantSql.PIPE_OWNERS_GET % processor.id
                cursor = django.db.connection.cursor()
                cursor.execute(tmp_sql)
                rows = cursor.fetchall()
                if len(rows) > 0:
                    read_id_list, write_id_list = self.get_owner_id_list(
                        processor.id,
                        tools_util.CONSTANTS.PROCESSOR)
                    for row in rows:
                        if str(row[0]) not in read_id_list \
                                and row[0] != processor.owner_id:
                            user_info = self.get_user_info_by_id(row[0])
                            return 1, ("算子被没有使用权限的用户 【%s】"
                                       "使用中，无法收回公共权限！" %
                                       user_info.username)
                processor.private = 1
            else:
                processor.private = 0

            processor.project_id = project_id
            processor.save()
            return 0, "OK"
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def delete_task_info(self, owner_id, task_id):
        try:
            with django.db.transaction.atomic():
                db_task = horae.models.Task.objects.get(id=task_id)
                if self.check_pipeline_auth_valid(
                        db_task.pl_id,
                        owner_id) != tools_util.UserPermissionType.WRITE:
                    return 1, "对不起，你没有权限删除这个DAG流的任务！"

                edges = horae.models.Edge.objects.filter(prev_task_id=task_id)
                edges.delete()
                edges = horae.models.Edge.objects.filter(next_task_id=task_id)
                edges.delete()
                db_task.delete()
                self.__graph_mgr.remove_node(str(task_id))

                return 0, "OK"
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    # def delete_task_info(self, owner_id, task_id):
    #     try:
    #         with django.db.transaction.atomic():
    #             db_task = horae.models.Task.objects.get(id=task_id)
    #             if self.check_pipeline_auth_valid(
    #                     db_task.pl_id,
    #                     owner_id) != tools_util.UserPermissionType.WRITE:
    #                 return 1, "对不起，你没有权限删除这个流程的任务！"

    #             next_task_ids = db_task.next_task_ids.split(',')
    #             other_dep_tasks = []
    #             for tmp_task_id in next_task_ids:
    #                 if tmp_task_id.strip() == '':
    #                     continue

    #                 task = horae.models.Task.objects.get(id=int(tmp_task_id))
    #                 if task.pl_id != db_task.pl_id:
    #                     pipeline = horae.models.Pipeline.objects.get(id=task.pl_id)
    #                     other_dep_tasks.append("流程：%s\t任务：%s" % (pipeline.name, task.name))
    #             if len(other_dep_tasks) > 0:
    #                 return 2, ("这个任务被其他流程的任务依赖，"
    #                            "请先解除依赖关系再删除。\n%s" %
    #                            '\n'.join(other_dep_tasks))

    #             run_tasks = horae.models.Schedule.objects.filter(
    #                 task_id=task_id,
    #                 status__in=(
    #                     tools_util.TaskState.TASK_READY,
    #                     tools_util.TaskState.TASK_RUNNING,
    #                     tools_util.TaskState.TASK_WAITING))
    #             if len(run_tasks) > 0:
    #                 return 1, "can't delete task, it is running!"

    #             run_historys = horae.models.RunHistory.objects.filter(
    #                 task_id=task_id)
    #             for run_history in run_historys:
    #                 run_history.delete()

    #             schedules = horae.models.Schedule.objects.filter(
    #                 task_id=task_id)
    #             for schedule in schedules:
    #                 schedule.delete()

    #             ready_tasks = horae.models.ReadyTask.objects.filter(
    #                 task_id=task_id)
    #             for ready_task in ready_tasks:
    #                 ready_task.delete()

    #             ordered_tasks = horae.models.OrderdSchedule.objects.filter(
    #                 task_id=task_id)
    #             for ordered_task in ordered_tasks:
    #                 ordered_task.delete()

    #             for tmp_task_id in next_task_ids:
    #                 if tmp_task_id.strip() == '':
    #                     continue
    #                 self.delete_edge(owner_id, task_id, int(tmp_task_id))

    #             prev_task_ids = db_task.prev_task_ids.split(',')
    #             for tmp_task_id in prev_task_ids:
    #                 if tmp_task_id.strip() == '':
    #                     continue
    #                 self.delete_edge(owner_id, int(tmp_task_id), task_id)

    #             db_task.delete()
    #             self.__graph_mgr.remove_node(str(task_id))

    #             return 0, "OK"
    #     except Exception as ex:
    #         self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
    #             str(ex), traceback.format_exc()))
    #         return 1, str(ex)

    def search_all_authed_processor(self, owner_id, word):
        try:
            processors_pub = horae.models.Processor.objects.filter(
                private=0,
                name__contains=word)
            processors_own = horae.models.Processor.objects.filter(
                owner_id=owner_id,
                private=1,
                name__contains=word)
            read_list, write_list = self.get_pipeline_id_list_by_owner_id(
                owner_id,
                tools_util.CONSTANTS.PROCESSOR)
            processors_other = horae.models.Processor.objects.filter(
                id__in=read_list,
                private=1,
                name__contains=word)
            res_map = {
                tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC: processors_own,
                tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC: processors_other,
                tools_util.PROCESSOR_TOP_TYPE.PUBLIC_PROC: processors_pub
            }
            return res_map
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def delete_processor(self, user_id, processor_id):
        try:
            is_super = is_admin(user_id)
            processor = horae.models.Processor.objects.get(id=processor_id)
            if not is_super and processor.owner_id != int(user_id):
                return 1, "必须是算子的创建者才能删除算子！"

            tasks = horae.models.Task.objects.filter(pid=processor_id)
            if len(tasks) > 0:
                return 1, "算子被其他任务依赖，不能直接删除算子！"
            perm_historys = models.PermHistory.objects.filter(
                resource_type=tools_util.CONSTANTS.PROCESSOR,
                resource_id=processor.id)
            for perm_history in perm_historys:
                perm_history.delete()

            processor.delete()
            return 0, "OK"
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    @django.db.transaction.atomic
    def delete_proc_version(self, user_id, proc_id, version_id, oss_bucket, oss_obj):
        is_super = is_admin(user_id)
        proc_info = horae.models.Processor.objects.get(id=proc_id)
        if not is_super and proc_info.owner_id != int(user_id):
            return 1, "必须是算子的创建者才能删除算子！"

        version_info = horae.models.UploadHistory.objects.get(id=version_id)
        version_info.delete()
        res = oss_bucket.delete_object(oss_obj)
        if res.status != 204:
            raise Exception("删除oss对象[%s]失败！" % oss_obj)

        return 0, "OK"

    def __check_has_project_auth(self, owner_id, project_id):
        try:
            project = horae.models.Project.objects.get(id=project_id)
            if project.owner_id == int(owner_id):
                return True

            tmp_sql = ("select max(id) from horae_permhistory where "
                       "resource_type = 'project' and resource_id = %s and "
                       "applicant_id = %s;" % (project_id, owner_id))
            cursor = django.db.connection.cursor()
            cursor.execute(tmp_sql)
            rows = cursor.fetchall()
            if len(rows) <= 0:
                return False

            if rows[0][0] is None or str(rows[0][0]) == 'null':
                return False

            com_perm = models.PermHistory.objects.get(id=rows[0][0])
            if com_perm.status == tools_util.AuthAction.GRANT_AUTH_TO_OTHER \
                    and com_perm.permission == \
                    tools_util.UserPermissionType.WRITE_STR:
                return True
            return False
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return False

    @django.db.transaction.atomic
    def delete_project(self, owner_id, project_id):
        try:
            with django.db.transaction.atomic():
                project = horae.models.Project.objects.get(id=project_id)
                if not self.__check_has_project_auth(owner_id, project_id):
                    return 1, "你没有这个项目的权限!"

                projects = horae.models.Project.objects.filter(
                    parent_id=project_id)
                if len(projects) > 0:
                    return 1, "这个项目下还有子类目，请先删除下面的子类目！"

                pipelines = horae.models.Pipeline.objects.filter(
                    project_id=project_id)
                if len(pipelines) > 0:
                    return 1, "这个项目下还有流程，请先删除流程！"

                processors = horae.models.Processor.objects.filter(
                    project_id=project_id)
                if len(processors) > 0:
                    return 1, "这个项目下还有算子，请先删除算子！"

                perm_list = models.PermHistory.objects.filter(
                    resource_type=tools_util.CONSTANTS.PROJECT,
                    resource_id=project_id)
                for perm in perm_list:
                    perm.delete()
                project.delete()
                return 0, "OK"
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return 1, str(ex)

    def get_manager_info_list(self, owner_list):
        try:
            users = django.contrib.auth.models.User.objects.filter(
                id__in=owner_list)
            return users
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def get_run_history_with_task_ids(
            self,
            task_id_list,
            min_run_time,
            max_run_time):
        try:
            tasks = horae.models.RunHistory.objects.exclude(
                status=tools_util.TaskState.TASK_SUCCEED).filter(
                run_time__lte=max_run_time,
                run_time__gte=min_run_time,
                task_id__in=task_id_list)
            if len(tasks) <= 0:
                return tasks
            return tasks
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def get_prev_nodes_info(self, prev_nodes):
        try:
            sql = ConstantSql.GET_CT_TIME_BY_TASK_ID_LIST % prev_nodes
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

    def get_task_history_by_pipe(self, pipeline_id, run_time):
        try:
            sql = ConstantSql.GET_TASK_BY_PIPELINE_ID % (
                pipeline_id,
                run_time)
            cursor = django.db.connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows
        except Exception as ex:
            self.__log.error("execute failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None
    
    def get_tasks_by_task_ids(self, task_ids):
        sql = ConstantSql.GET_TASK_INFO_WITH_PIPELINE_NAME % task_ids
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as ex:
            self.__log.error("execute[%s] failed![ex:%s][trace:%s]!" % (
                sql, str(ex), traceback.format_exc()))
            return None

    def get_runhsitory_with_id(self, schedule_id):
        try:
            return horae.models.RunHistory.objects.get(
                    schedule_id=schedule_id)
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s]"
                    "[trace:%s][schedule_id: %s]!" % (
                    str(ex), traceback.format_exc(), schedule_id))
            return None

    def get_rerunhsitory_with_id(self, rerun_id):
        try:
            return horae.models.RerunHistory.objects.get(
                    id=rerun_id)
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s]"
                    "[trace:%s][schedule_id: %s]!" % (
                    str(ex), traceback.format_exc(), rerun_id))
            return None

    @django.db.transaction.atomic
    def pipeline_off_or_on_line(self, owner_id, pipeline_id, on_line):
        """
            如果是下线，需要将正在运行中的任务全部标记为用户停止状态
        """
        try:
            with django.db.transaction.atomic():
                pipeline = horae.models.Pipeline.objects.get(
                        id=pipeline_id)
                if pipeline.enable == on_line:
                    return 1, (
                            'pipeline on line status[%s] '
                            'equal to gived: %s' % (pipeline.enable, on_line))

                if self.check_pipeline_auth_valid(
                        pipeline_id,
                        owner_id) != tools_util.UserPermissionType.WRITE:
                    return 1, "对不起，你没有权限对这个流程进行上下线操作!"

                tasks = horae.models.Task.objects.filter(pl_id=pipeline_id)
                now_pipe_tasks = set()
                for task in tasks:
                    now_pipe_tasks.add(task.id)

                if on_line == 0:
                    schedules = horae.models.Schedule.objects.filter(
                            pl_id=pipeline_id,
                            status__in=(
                                    tools_util.TaskState.TASK_READY,
                                    tools_util.TaskState.TASK_RUNNING,
                                    tools_util.TaskState.TASK_WAITING))
                    for schedule in schedules:
                        schedule.status = \
                                tools_util.TaskState.TASK_STOPED_BY_USER
                        schedule.save()

                    run_historys = horae.models.RunHistory.objects.filter(
                            pl_id=pipeline_id,
                            status__in=(
                                    tools_util.TaskState.TASK_READY,
                                    tools_util.TaskState.TASK_RUNNING,
                                    tools_util.TaskState.TASK_WAITING))
                    for run_history in run_historys:
                        run_history.status = \
                                tools_util.TaskState.TASK_STOPED_BY_USER
                        run_history.save()

                    ready_tasks = horae.models.ReadyTask.objects.filter(
                            pl_id=pipeline_id,
                            status__in=(
                                    tools_util.TaskState.TASK_READY,
                                    tools_util.TaskState.TASK_RUNNING))
                    for ready_task in ready_tasks:
                        ready_task.status = \
                                tools_util.TaskState.TASK_STOPED_BY_USER
                        ready_task.save()

                    ready_tasks = horae.models.ReadyTask.objects.filter(
                            pl_id=pipeline_id,
                            status=tools_util.TaskState.TASK_FAILED)
                    for ready_task in ready_tasks:
                        ready_task.status = \
                                tools_util.TaskState.TASK_STOPED_BY_USER
                        ready_task.save()
                        schedule = horae.models.Schedule.objects.get(
                                id=ready_task.schedule_id)
                        schedule.status = \
                                tools_util.TaskState.TASK_STOPED_BY_USER
                        schedule.save()
                        run_history = horae.models.RunHistory.objects.get(
                                schedule_id=ready_task.schedule_id)
                        run_history.status = \
                                tools_util.TaskState.TASK_STOPED_BY_USER
                        run_history.save()

                if on_line == 1:
                    run_time = tools_util.StaticFunction.get_now_format_time(
                            "%Y%m%d%H%M")
                    for task in tasks:
                        task.last_run_time = run_time
                        task.save()

                pipeline.enable = on_line
                # if on_line == 1:
                #     is_super = is_admin(owner_id)
                #     if is_super != 1:
                #         pipeline.enable = 2

                pipeline.save()
                return 0, "OK"
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return 1, str(ex)

    # @django.db.transaction.atomic
    # def copy_pipeline(self, owner_id, src_pl_id, new_pl_name, project_id, use_type_src):
    #     try:
    #         with django.db.transaction.atomic():
    #             new_pl_name = new_pl_name.strip()
    #             new_pl_name = new_pl_name.replace('\r', '')
    #             new_pl_name = new_pl_name.replace('\n', '')
    #             pipeline = horae.models.Pipeline.objects.get(id=src_pl_id)
    #             if pipeline.name == new_pl_name:
    #                 return 1, "流程名重名"

    #             pipe_type = pipeline.type
    #             if not use_type_src:
    #                 pipe_type = 0

    #             status, info = self.create_new_pipeline(
    #                 name=new_pl_name,
    #                 ct_time=pipeline.ct_time,
    #                 owner_id=owner_id,
    #                 manager_id_list='',
    #                 monitor_way=pipeline.monitor_way,
    #                 tag=pipeline.tag,
    #                 description=pipeline.description,
    #                 life_cycle=pipeline.life_cycle,
    #                 type=pipe_type,
    #                 project_id=project_id)
    #             if status != 0:
    #                 raise Exception(
    #                     "copy new pipeline failed![%s]" % info)

    #             new_pipeline = horae.models.Pipeline.objects.get(
    #                 name=new_pl_name)
    #             src_tasks = horae.models.Task.objects.filter(pl_id=src_pl_id)
    #             old_task_map = {}
    #             new_task_map = {}
    #             old_new_task_id_map = {}
    #             old_task_id_set = set()
    #             old_task_id_list = []
    #             for src_task in src_tasks:
    #                 new_task = horae.models.Task(
    #                     pl_id=new_pipeline.id,
    #                     pid=src_task.pid,
    #                     next_task_ids='',
    #                     prev_task_ids='',
    #                     over_time=src_task.over_time,
    #                     name=src_task.name,
    #                     config=src_task.config,
    #                     retry_count=src_task.retry_count,
    #                     last_run_time=src_task.last_run_time,
    #                     description=src_task.description,
    #                     priority=src_task.priority,
    #                     except_ret=src_task.except_ret,
    #                     server_tag=src_task.server_tag,
    #                     version_id=src_task.version_id)
    #                 status, add_task = self.add_new_task_to_pipeline(
    #                     owner_id,
    #                     new_task,
    #                     None)
    #                 if status != 0:
    #                     self.delete_pipeline(owner_id, new_pipeline.id)
    #                     raise Exception(add_task)
    #                 new_task = horae.models.Task.objects.get(
    #                     pl_id=new_pipeline.id,
    #                     name=src_task.name)
    #                 old_task_map[new_task.id] = src_task
    #                 new_task_map[new_task.id] = new_task
    #                 old_task_id_set.add(src_task.id)
    #                 old_task_id_list.append(src_task.id)
    #                 old_new_task_id_map[src_task.id] = new_task.id

    #             edges = horae.models.Task.objects.filter(prev_task_id__in=old_task_id_list)
    #             for edge in edges:
    #                 if edge.next_task_id in old_task_id_set:
    #                     self.add_edge()
    #             for task_id in new_task_map:
    #                 prev_id_str = ''
    #                 if old_task_map[task_id].prev_task_ids is not None \
    #                         and old_task_map[task_id].prev_task_ids != '':
    #                     prev_id_list = []
    #                     old_prev_task_id_list = \
    #                         old_task_map[task_id].prev_task_ids.split(',')
    #                     for old_prev_id in old_prev_task_id_list:
    #                         if old_prev_id.strip() == "":
    #                             continue

    #                         if int(old_prev_id) not in old_task_id_set:
    #                             continue
    #                         prev_id_list.append(
    #                             str(old_new_task_id_map[int(old_prev_id)]))

    #                     prev_id_str = ",".join(prev_id_list)

    #                 next_id_str = ''
    #                 if old_task_map[task_id].next_task_ids is not None \
    #                         and old_task_map[task_id].next_task_ids != '':
    #                     next_id_list = []
    #                     old_next_task_id_list = \
    #                         old_task_map[task_id].next_task_ids.split(',')
    #                     for old_next_id in old_next_task_id_list:
    #                         if old_next_id.strip() == "":
    #                             continue

    #                         if int(old_next_id) not in old_task_id_set:
    #                             continue
    #                         next_id_list.append(
    #                             str(old_new_task_id_map[int(old_next_id)]))
    #                     next_id_str = ",".join(next_id_list)
    #                 new_task_map[task_id].prev_task_ids = prev_id_str
    #                 new_task_map[task_id].next_task_ids = next_id_str
    #                 new_task_map[task_id].save()
    #             return 0, new_pipeline.id
    #     except Exception as ex:
    #         self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
    #             str(ex), traceback.format_exc()))
    #         return 1, str(ex)

    def copy_task(self, owner_id, src_task_id, dest_pl_id):
        if self.check_pipeline_auth_valid(
                dest_pl_id,
                owner_id) != tools_util.UserPermissionType.WRITE:
            return 1, "你没有权限将任务拷贝到目标DAG流！"
        try:
            src_task = horae.models.Task.objects.get(id=src_task_id)
            new_task_name = "%s_copy" % src_task.name
            new_task = horae.models.Task(
                    pl_id=dest_pl_id,
                    pid=src_task.pid,
                    next_task_ids='',
                    prev_task_ids='',
                    over_time=src_task.over_time,
                    name=new_task_name,
                    config=src_task.config,
                    retry_count=src_task.retry_count,
                    last_run_time=src_task.last_run_time,
                    description=src_task.description,
                    priority=src_task.priority,
                    except_ret=src_task.except_ret,
                    server_tag=src_task.server_tag,
                    version_id=src_task.version_id)
            status, add_task = self.add_new_task_to_pipeline(
                    owner_id,
                    new_task,
                    None)
            if status != 0:
                return 1, add_task
            return 0, new_task
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return 1, str(ex)
