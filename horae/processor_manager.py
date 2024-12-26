###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import os
import json
import configparser
import shutil
import copy
import time

#import oss2
import horae
import django
from django.conf import settings

from horae import tools_sql_manager
from horae import tools_util
from common.util import is_admin
from horae import no_block_sys_cmd

class ProcessorManager(object):
    """
        流程管理接口实现
    """
    def __init__(self, logger):
        self.__log = logger
        self.__sql_manager = tools_sql_manager.SqlManager(logger)
        self.__work_package_dir = settings.WORK_PACKAGE_DIR
        self.__shell_cmd = no_block_sys_cmd.NoBlockSysCommand()

        config = configparser.ConfigParser()
        config.read("./conf/tools.conf")
        self.__package_path = config.get("tools", "package_path").strip()
        '''
        oss_id = config.get("tools", "oss_id").strip()
        oss_key = config.get("tools", "oss_key").strip()
        oss_host = config.get("tools", "oss_host").strip()
        self.__oss_dbname = config.get("tools", "oss_dbname").strip()
        self.__oss_package_dir = config.get("tools", "oss_prefix").strip()
        self.__auth = oss2.Auth(oss_id, oss_key)
        self.__bucket = oss2.Bucket(self.__auth, oss_host, self.__oss_dbname)
        '''

    def get_processor_info(self, processor_id):
        processor = self.__sql_manager.get_proessor_info(processor_id)
        if processor is None:
            return self.__get_default_ret_map(1, "db error!")
        proc_map = {}
        proc_map["id"] = processor.id
        proc_map["name"] = processor.name
        proc_map["type"] = processor.type
        proc_map["template"] = processor.template
        proc_map["update_time"] = processor.update_time.strftime(
                "%Y-%m-%d %H:%M:%S")
        proc_map["description"] = processor.description
        proc_map["config"] = processor.config
        proc_map["input_config"] = processor.input_config
        proc_map["output_config"] = processor.output_config
        proc_map["owner_id"] = processor.owner_id
        proc_map["private"] = processor.private
        proc_map["tag"] = processor.tag
        proc_map["tpl_files"] = processor.tpl_files
        read_id_list, write_id_list = self.__sql_manager.get_owner_id_list(
                processor.id,
                tools_util.CONSTANTS.PROCESSOR)
        user_name_list = []
        for user_id in read_id_list:
            user = self.__sql_manager.get_user_info_by_id(user_id)
            if user is not None:
                user_name_list.append(user.username)

        proc_map["user_list"] = ",".join(user_name_list)
        proc_map["userid_list"] = ",".join(read_id_list)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["processor"] = proc_map

        return json.dumps(ret_map)

    def __get_default_ret_map(self, status, info):
        ret_map = {}
        ret_map["status"] = status
        ret_map["info"] = info
        return json.dumps(ret_map)

    def get_processor_package_history(self, processor_id):
        upload_historys = self.__sql_manager.get_processor_package_history(
                processor_id)
        if upload_historys is None:
            return self.__get_default_ret_map(1, "db error!")

        history_list = []
        for history in upload_historys:
            tmp_map = {}
            tmp_map["id"] = history[0]
            tmp_map["upload_time"] = history[1].strftime("%Y-%m-%d %H:%M:%S")
            tmp_map["upload_user_id"] = history[2]
            tmp_map["upload_user_name"] = history[3]
            tmp_map["version"] = history[4]
            tmp_map["status"] = history[5]
            tmp_map["description"] = history[6]
            tmp_map["update_time"] = history[7].strftime("%Y-%m-%d %H:%M:%S")
            tmp_map["name"] = history[8]
            tmp_map["type"] = history[9]
            history_list.append(tmp_map)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["package_historys"] = history_list
        print("get upload history:")
        print(ret_map)
        return json.dumps(ret_map)

    def get_processor_quote_num(self, processor_id):
        quote_num = self.__sql_manager.get_processor_quote_num(processor_id)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["quote_num"] = quote_num
        return json.dumps(ret_map)

    def get_processor_quote_list(self, processor_id):
        quote_list = self.__sql_manager.get_processor_quote_list(processor_id)
        if quote_list is None:
            return self.__get_default_ret_map(1, "db error!")
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["quote_list"] = quote_list
        return json.dumps(ret_map)

    def update_processor(self, user_id, processor, user_id_list):
        status, info = self.__sql_manager.update_processor(
                user_id,
                processor,
                user_id_list)
        return self.__get_default_ret_map(status, info)

    def create_processor(self, processor, user_id_list):
        status, info = self.__sql_manager.create_processor(
                processor,
                user_id_list)
        return self.__get_default_ret_map(status, info)

    def rec_get_children(self, parent, tree_map, ret_children, remove_empty_child_node=False):
        if parent not in tree_map or len(tree_map[parent]) <= 0:
            return False

        for item in tree_map[parent]:
            item["children"] = []
            res = self.rec_get_children(item["id"], tree_map, item["children"], remove_empty_child_node)
            if item["is_project"]  and remove_empty_child_node and not res:
                continue

            ret_children.append(item)

        if len(ret_children) <= 0:
            return False

        return True

    def get_proc_project_tree(self, user_id, type=-1):
        if type == tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC:
            projects = horae.models.Project.objects.filter(type=1, owner_id=user_id)
        else:
            projects = horae.models.Project.objects.filter(type=1)

        tree_map = {}
        for project in projects:
            if project.parent_id not in tree_map:
                tree_map[project.parent_id] = []

            tree_map[project.parent_id].append({
                "id": project.id, "text": project.name, "is_project": 1})

        ret_list = []
        if type in tree_map:
            for item in tree_map[type]:
                ret_list.append(item)
                item["children"] = []
                self.rec_get_children(item["id"], tree_map, item["children"])

        root_map = {"id": type, "text": tools_util.global_processor_top_type_map[type], "is_project": 1, "children": ret_list}
        return [root_map]

    def get_shared_processor_tree_async(self, user_id, tree_id):
        if tree_id != -2:
            return []

        projects = horae.models.Project.objects.filter(type=1)
        tree_map = {}
        for project in projects:
            if project.parent_id not in tree_map:
                tree_map[project.parent_id] = []

            tree_map[project.parent_id].append({
                "id": project.id, "text": project.name, "is_project": 1})

        processors = self.__sql_manager.show_processor_own_public(
            user_id, 0, 1000, 'update_time', 'desc', '')

        ret_list = []
        for processor in processors:
            if processor[12] == tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC:
                ret_list.append({
                    "id": "%s-%s" % (tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC, processor[0]),
                    "text": processor[1], "is_project": 0})
            else:
                if processor[12] not in tree_map:
                    tree_map[processor[12]] = []

                tree_map[processor[12]].append(
                    {"id": "%s-%s" % (processor[12], processor[0]),
                     "text": processor[1], "is_project": 0})

        if tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC in tree_map:
            for item in tree_map[tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC]:
                item["children"] = []
                self.rec_get_children(item["id"], tree_map, item["children"], remove_empty_child_node=True)
                if item["is_project"] == 1 and len(item["children"]) <= 0:
                    continue

                ret_list.append(item)

        return ret_list

    def get_standard_processor_tree_async(self, user_id, tree_id):
        if tree_id != tools_util.PROCESSOR_TOP_TYPE.PUBLIC_PROC:
            return []

        is_super = is_admin(user_id)
        projects = self.__sql_manager.get_projects_with_parent_id(parent_id=tree_id, type=1)
        res_list = []
        for project in projects:
            res_list.append({"id": project.id, "text": project.name, "state": "closed",
                             "is_project": 2, "is_super": is_super})

        if int(tree_id) != 0:
            processors = self.__sql_manager.get_processor_with_project_id(project_id=tree_id)
            for proc in processors:
                res_list.append({
                    "id": "%s-%s" % (tree_id, proc.id),
                    "text": proc.name,
                    "state": "open",
                    "is_project": 0,
                    "iconCls": "icon-file"
                })

        return res_list

    def public_processor(self, processor_id, project_id):
        status, info = self.__sql_manager.public_processor(processor_id, project_id)
        return self.__get_default_ret_map(status, info)

    def get_processor_tree_async(self, user_id, tree_id):
        is_super = is_admin(user_id)
        if tree_id == -1 and not is_super:
            projects = self.__sql_manager.get_projects_with_parent_id(owner_id=user_id, parent_id=tree_id, type=1)
        else:
            projects = self.__sql_manager.get_projects_with_parent_id(parent_id=tree_id, type=1)

        res_list = []
        for project in projects:
            res_list.append({"id": project.id, "text": project.name, "state": "closed", "is_project": 1})

        if int(tree_id) != 0:
            if tree_id == tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC and not is_super:
                processors = self.__sql_manager.get_processor_with_project_id(
                    project_id=tree_id, owner_id=user_id)
            else:
                processors = self.__sql_manager.get_processor_with_project_id(project_id=tree_id)

            for proc in processors:
                res_list.append({
                    "id": "%s-%s" % (tree_id, proc.id),
                    "text": proc.name,
                    "state": "open",
                    "is_project": 0,
                    "iconCls": "icon-file"
                })

        return res_list

    def get_git_code(self, proc_id, git_url):
        git_url = git_url.strip()
        if not git_url.endswith(".git"):
            raise Exception("不是合法的git路径。")

        git_url = git_url.replace("https://github.com", "https://actantion:qincai521@github.com")
        file_path = os.path.join(self.__package_path, str(proc_id), "git")
        if os.path.exists(file_path):
            shutil.rmtree(file_path)

        os.makedirs(file_path)
        cmd = "cd %s && git clone %s" % (file_path, git_url)
        stdout, stderr, return_code = no_block_sys_cmd.NoBlockSysCommand().run_once(cmd)
        if return_code != 0:
            raise Exception("run cmd[%s] failed error[%s]" % (cmd, stderr))

        cmd = "cd %s && tar -zcvf ./%s.tar.gz ./*" % (file_path, proc_id)
        stdout, stderr, return_code = no_block_sys_cmd.NoBlockSysCommand().run_once(cmd)
        if return_code != 0:
            raise Exception("run cmd[%s] failed error[%s]" % (cmd, stderr))

        return os.path.join(file_path, "%s.tar.gz" % proc_id)

    @django.db.transaction.atomic
    def upload_package_with_local(self, user_id, proc_id, version_name, desc, git_url, file_name, type):
        # if git_url is not None and type == 1:
        #     file_name = self.get_git_code(proc_id, git_url)

        if type == 0 and (file_name is None or file_name.strip() == ""):
            raise Exception("没有合法的git_url或者本地文件上传。")

        his_uploads = horae.models.UploadHistory.objects.filter(
            processor_id=proc_id, name=version_name)
        if len(his_uploads) > 0:
            upload_his = his_uploads[0]
            upload_his.upload_time = tools_util.StaticFunction.get_now_format_time("%Y-%m-%d %H:%M:%S")
            upload_his.description = desc
            upload_his.git_url = git_url
            upload_his.type = type
            upload_his.save()
        else:
            upload_his = horae.models.UploadHistory(
                processor_id=proc_id,
                status=0,
                update_time=tools_util.StaticFunction.get_now_format_time("%Y-%m-%d %H:%M:%S"),
                upload_time=tools_util.StaticFunction.get_now_format_time("%Y-%m-%d %H:%M:%S"),
                upload_user_id=user_id,
                version=version_name,
                name=version_name,
                description=desc,
                git_url=git_url,
                type = type)
            upload_his.save()

        if type == 0:
            save_id = "%s-%s" % (proc_id, upload_his.id)
            self.__shell_cmd.run_once("rm -rf %s/%s.tar.gz" % (self.__work_package_dir, save_id))
            self.__shell_cmd.run_once("mv %s %s/%s.tar.gz" % (file_name, self.__work_package_dir, save_id))
            self.__log.error("type: %d, upload work pacakge success! %s %s/%s.tar.gz" % (type, file_name, self.__work_package_dir, save_id))
        else:
            self.__log.error("not upload type: %d work pacakge success! %s %s/%s.tar.gz" % (type, file_name, self.__work_package_dir, save_id))

        '''
        file_object = self.__oss_package_dir + "/" + save_id + ".tar.gz"
        res = self.__bucket.put_object_from_file(file_object, file_name)
        if res.status != 200:
            raise Exception("将包上传oss失败！")
       '''
        return {"status": 0, "msg": "OK"}

    def search_processor(self, user_id, word, with_project, limit):
        projects = self.__sql_manager.get_all_projects(type=1)
        tree_map = {}
        tree_map[tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC] = []
        tree_map[tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC] = []
        tree_map[tools_util.PROCESSOR_TOP_TYPE.PUBLIC_PROC] = []
        for project in projects:
            if project.parent_id not in tree_map:
                tree_map[project.parent_id] = []

            if project.id not in tree_map:
                tree_map[project.id] = []

            tree_map[project.parent_id].append({
                "id": project.id, "text": project.name,
                "is_project": 1, "state": "open"})

        ret_list = []
        processors_map = self.__sql_manager.search_all_authed_processor(user_id, word)
        for key in range(-1, -4, -1):
            processors = processors_map[key]
            tmp_map = copy.deepcopy(tree_map)
            for processor in processors:
                tmp_map[processor.project_id].append({
                    "id": "%s-%s" % (processor.project_id, processor.id),
                    "text": processor.name, "is_project": 0, "iconCls": "icon-file"})
    
            text_name = tools_util.global_processor_top_type_map[key]
            item = {"id": key, "text": text_name, "is_project": 1, "state": "open"}
            item["children"] = []
            if key == tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC:
                res = self.rec_get_children(
                    tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC, tmp_map, item["children"], True)
            else:
                res = self.rec_get_children(item["id"], tmp_map, item["children"], True)

            print(key, ":",  len(processors), ",", item, ":", res)

            if res:
                ret_list.append(item)

        return ret_list

    def get_proc_with_project_tree(self, user_id, id):
        processor = horae.models.Processor.objects.get(id=id)
        parent_id = processor.project_id
        res_list = []
        res_list.append(
            {"id": "%s-%s" % (parent_id, processor.id),
             "text": processor.name,
             "is_project": 0,
             "state": "open",
             "iconCls": "icon-file"
             })

        while parent_id > 0:
            project = horae.models.Project.objects.get(id=parent_id)
            tmp_list = []
            tmp_list.append({
                "id": "%s" % project.id, "text": project.name,
                "is_project": 1, "children": res_list})
            res_list = tmp_list
            parent_id = project.parent_id


        tmp_map = {}
        if parent_id == tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC:
            if processor.owner_id == user_id:
                tmp_map = {
                    "id": "%s" % tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC,
                    "text": tools_util.global_processor_top_type_map[tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC],
                    "is_project": 1, "children": res_list}
            else:
                read_id_list, write_id_list = self.__sql_manager.get_owner_id_list(
                    processor.id,
                    tools_util.CONSTANTS.PROCESSOR)
                is_read_user = False
                for id in read_id_list:
                    if id == user_id:
                        is_read_user = True
                        break

                if is_read_user:
                    tmp_map = {
                        "id": "%s" % tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC,
                        "text": tools_util.global_processor_top_type_map[tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC],
                        "is_project": 1, "children": res_list}
                else:
                    user = self.__sql_manager.get_user_info_by_id(user_id)
                    if user is not None:
                        if user.first_name is not None and user.first_name.strip() != "":
                            username = user.first_name.strip()
                        else:
                            username = user.username
                    else:
                        username = "未知用户"

                    tmp_map = {
                        "id": "%s" % tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC,
                        "text": "%s的算子" % username,
                        "is_project": 1, "children": res_list}
        else:
            tmp_map = {
                "id": "%s" % tools_util.PROCESSOR_TOP_TYPE.PUBLIC_PROC,
                "text": tools_util.global_processor_top_type_map[tools_util.PROCESSOR_TOP_TYPE.PUBLIC_PROC],
                "is_project": 1, "children": res_list}


        res = {"status": 0, "msg": "OK", "node_id": "%s-%s" % (
            processor.project_id, processor.id), "data": [tmp_map]}
        return res

    def delete_processor(self, user_id, processor_id):
        status, info = self.__sql_manager.delete_processor(user_id, processor_id)
        return self.__get_default_ret_map(status, info)

    def delete_proc_version(self, user_id, proc_id, id):
        oss_obj = os.path.join(
            self.__oss_package_dir,
            "%s-%s.tar.gz" % (proc_id, id))
        status, msg = self.__sql_manager.delete_proc_version(
            user_id, proc_id, id, self.__bucket, oss_obj)
        return {"status": status, "msg": msg}

    @django.db.transaction.atomic
    def upload_pacakge(
            self,
            processor_id,
            owner_id,
            file_path,
            version_name,
            description=None,
            overwrite=False):
        processor = self.__sql_manager.get_proessor_info(processor_id)
        if processor is None:
            self.__log.error("get processor info failed!")
            return self.__get_default_ret_map(1, "get processor info failed!")

        now_time = tools_util.StaticFunction.get_now_format_time(
            "%Y-%m-%d %H:%M:%S")
        if description is None:
            description = ''

        upload_histories = horae.models.UploadHistory.objects.filter(
            processor_id=processor_id, name=version_name)
        if len(upload_histories) > 0:
            upload_history = upload_histories[0]
            upload_history.upload_time = now_time
            upload_history.description = description
            upload_history.save()
        else:
            upload_history = horae.models.UploadHistory(
                processor_id=processor_id,
                status=1,
                update_time=now_time,
                upload_time=now_time,
                upload_user_id=owner_id,
                version=version_name,
                name=version_name,
                description=description)
            upload_history.save()

        if not self.__handle_genaral_package(
                processor.type,
                processor_id,
                upload_history.id,
                file_path,
                overwrite):
            self.__log.error("hand oss package failed!")
            raise Exception("将包写入oss失败")

        return self.__get_default_ret_map(0, "OK")

    def __handle_genaral_package(
            self,
            task_type,
            processor_id,
            upload_id,
            file_path,
            overwrite):
        save_id = "%s-%s" % (processor_id, str(upload_id))
        file_object = self.__oss_package_dir + "/" + save_id + ".tar.gz"
        res = self.__bucket.put_object_from_file(
            file_object, "%s/%s.tar.gz" % (file_path, processor_id))
        if res.status != 200:
            self.__log.error("upload work pacakge failed!file_object[%s], src[%s]" % (
                file_object, "%s/%s.tar.gz" % (file_path, processor_id)))
            return False

        return True
