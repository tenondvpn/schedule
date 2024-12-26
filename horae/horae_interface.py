###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import threading
import traceback
import json

from horae import pipeline_manager
from horae import processor_manager
from horae import tools_util
from horae import common_logger
from horae import run_history_manager

class HoraeInterface(tools_util.Singleton):
    """
        流程前端工具接口
    """
    mutex = threading.Lock()
    def __init__(self, logger=None):
        if not hasattr(self, "a"):
            tools_util.AutoLock(HoraeInterface.mutex)
            if not hasattr(self, "a"):
                if logger is not None:
                    horae_logger = logger
                else:
                    horae_logger = common_logger.get_logger(
                            "common", 
                            "./log/horae")

                self.__pipeline_mgr = pipeline_manager.PipelineManager(
                        horae_logger)
                self.__processor_mgr = processor_manager.ProcessorManager(
                        horae_logger)
                self.__run_history = run_history_manager.RunHistoryManager(
                        horae_logger,
                        self.__pipeline_mgr)
                self.__log = common_logger.get_logger(
                        "user_log", 
                        "./log/user_log")
                self.a = "a"  # 必须在后面

    def get_project_tree(self, user_id, type=0):
        """
            获取流程分类树

            Args:
                user_id: int 流程原始owner

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_project_tree(user_id, type)
        use_time = time.time() - beign_time
        self.__log.info("get_project_tree user_id: %s, type: %s, ret: %s[use_time:%s]"
                        % (user_id, type, ret, use_time))
        return ret

    def search_pipeline(self, user_id, word, with_project=1, limit=100):
        """
            搜索流程信息

            Args:
                user_id: int 流程原始owner
                word: string 搜索关键词

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.search_pipeline(user_id, word, with_project, limit)
        use_time = time.time() - beign_time
        self.__log.info("search_pipeline user_id: %s, word: %s, ret: %s[use_time:%s]"
                        % (user_id, word, ret, use_time))
        return ret

    def get_project_tree_async(self, user_id, tree_id, type):
        """
            获取流程分类树，异步获取

            Args:
                user_id: int 流程原始owner
                tree_id: int 项目树id

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_project_tree_async(user_id, tree_id, type)
        use_time = time.time() - beign_time
        self.__log.info("get_project_tree_async user_id: %s, tree_id: %s,"
                        " type: %s, ret: %s[use_time:%s]"
                        % (user_id, tree_id, type, ret, use_time))
        return ret

    def get_pipeline_info(self, pipeline_id):
        """
        通过pipeline id获取流程详情

        Args:
            pipeline_id: int pipeline id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "pipeline":
                {
                    "id": 0,
                    "name": "test",
                    "ct_time": "8 * * * *",
                    "update_time": "2015-06-17 10:00:00",
                    "enable": 1,
                    "type": 0,
                    "email_to": "lei.xie@eigen.com",
                    "description": "test",
                    "sms_to": "13521170126",
                    "tag": "a,b,c,d",
                    "life_cycle": 10,
                    "monitor_way": 2,
                    "project_id": 2,
                    "project_name": 'test_project',
                    "owner_list": [
                        {
                            "id": 1,  # userid
                            "username": "test_user"  # username
                       },
                    ]
               },
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_pipeline_info(pipeline_id)
        use_time = time.time() - beign_time
        self.__log.info("get_pipeline_info  pipeline_id: %s, "
                "ret: %s[use_time:%s]" % (
                pipeline_id, ret, use_time))
        return ret

    def get_tasks_by_pipeline_id(self, pipeline_id):
        """
        通过pipeline id获取这个流程的所有任务数据

        Args:
            pipeline_id: int pipeline id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "tasks": [
                    {
                        "id": 1,
                        "pl_id": 1,
                        "pid": 1,
                        "next_task_ids": "2,3",
                        "prev_task_ids": "4,",
                        "over_time": 12,
                        "name": "test_task",
                        "config": "input_file = oss://AY500/test/test.* \n
                                  output_file = oss://AY500/test/test.log",
                        "retry_count": 3,
                        "last_run_time": "20150516100000",
                        "description": "test",
                        "priority": 6,
                    },
                ]
            }

            返回一个list，每一个元素是一个Task实例(详见horae/models.py)

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_tasks_by_pipeline_id(pipeline_id)
        use_time = time.time() - beign_time
        self.__log.info("get_tasks_by_pipeline_id pipeline_id: %s, "
                        "ret: %s[use_time:%s]"
                        % (pipeline_id, ret, use_time))
        return ret

    def delete_edge(self, owner_id, from_task_id, to_task_id):
        """
        在流程图中删除一条边

        Args:
            owner_id: int owner_id
            from_task_id：int 边的上游task_id
            to_task_id：int 边的下游task_id

        Returns:
            返回一个状态，以及一个字符串，
            状态为0表示成功，其他失败
            字符串表示失败原因，成功则为‘OK’
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.delete_edge(
                owner_id,
                from_task_id,
                to_task_id)
        use_time = time.time() - beign_time
        self.__log.info("delete_edge owner_id: %s, "
                "from_task_id: %s, to_task_id: %s, ret: %s[use_time:%s]"
                 % (owner_id, from_task_id, to_task_id, ret, use_time))
        return ret

    def add_edge(self, owner_id, from_task_id, to_task_id, edge):
        """
        在流程图中添加一条边

        Args:
            owner_id: int owner_id
            from_task_id：int 边的上游task_id
            to_task_id：int 边的下游task_id

        Returns:
            返回一个状态，以及一个字符串，
            状态为0表示成功，其他失败
            字符串表示失败原因，成功则为‘OK’
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.add_edge(owner_id, from_task_id, to_task_id, edge)
        use_time = time.time() - beign_time
        self.__log.info("add_edge owner_id: %s, "
                "from_task_id: %s, to_task_id: %s, ret: %s[use_time:%s]"
                 % (owner_id, from_task_id, to_task_id, ret, use_time))
        return ret

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
        """
        修改一个pipeline，如果参数为None则相关字段不做修改

        Args:
            pipeline_id: pipeline_id
            owner_id：int 创建者id
            lifecycle：int 生命周期
            name: string pipeline名字
            ct_time: string 调度时间
            manager_id_list: int 拥有管理权限的用户id列表
            monitor_way: int 报警方式0邮件，1钉钉，2二者都报
            tag: string 标签, 多个标签','分隔
            description: string 描述
            type: int 类型
            project_id：int 项目id

        Returns:
            {
                "status": 0, # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.update_pipeline(
                pipeline_id,
                owner_id,
                lifecycle,
                name,
                ct_time,
                manager_id_list,
                monitor_way,
                tag,
                description,
                type,
                project_id)
        use_time = time.time() - beign_time
        self.__log.info("update_pipeline owner_id: %s, "
                "pipeline_id: %s, name: %s, ct_time: %s, manager_id_list: %s,"
                " monitor_way: %s, tag: %s, description:%s, lifecycle: %s, "
                "project_id: %s, ret: %s[use_time:%s]" % (
                owner_id, pipeline_id, name, ct_time, str(manager_id_list),
                monitor_way, tag, description, lifecycle,
                project_id, ret, use_time))
        return ret

    def copy_pipeline(
            self,
            owner_id,
            src_pl_id,
            new_pl_name,
            project_id,
            use_type_src=False):
        """
        拷贝流程

        Args:
            owner_id: int 用户id
            src_pl_id： int 被拷贝流程id
            new_pl_name：string 新流程名
            project_id：int 项目id
            use_type_src: bool True流程类型使用被拷贝流程的类型,False使用0

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "pl_id": 1,  # 新建的流程id
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.copy_pipeline(
                owner_id,
                src_pl_id,
                new_pl_name,
                project_id,
                use_type_src)
        use_time = time.time() - beign_time
        self.__log.info("copy_pipeline owner_id: %s, src_pl_id: %s, "
                 "new_pl_name: %s, use_type_src: %s, ret: %s[use_time:%s]"
                 % (owner_id, src_pl_id, new_pl_name,
                 use_type_src, ret, use_time))
        return ret

    def delete_pipeline(self, owner_id, pipeline_id):
        """
        删除一个pipeline

        Args:
            owner_id: int owner_id（包括创建者和相关拥有写权限的用户id）
            pipeline_id: int pipeline id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.delete_pipeline(owner_id, pipeline_id)
        use_time = time.time() - beign_time
        self.__log.info("delete_pipeline owner_id: %s, "
                "pipeline_id: %s  ret: %s[use_time:%s]" % (
                owner_id, pipeline_id, ret, use_time))
        return ret

    def update_tasks(self, owner_id, task, old_task=None, template=None):
        """
        修改一个task

        Args:
            owner_id: int owner_id
            task：修改后的信息，
            old_task: 是修改前信息，如果为None则强制修改task
                    task和old_task为Task实例(详见horae/models.py)

        Returns:
            返回一个状态，以及一个字符串，
            状态为0表示成功，其他失败
            字符串表示失败原因，成功则为‘OK’
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "task":
                {
                    "id": 1,
                    "pl_id": 1,
                    "pid": 1,
                    "next_task_ids": "2,3",
                    "prev_task_ids": "4,",
                    "over_time": 12,
                    "name": "test_task",
                    "config": "input_file = oss://AY500/test/test.* \n
                                output_file = oss://AY500/test/test.log",
                    "retry_count": 3,
                    "last_run_time": "20150516100000",
                    "description": "test",
                    "priority": 6,
                    "server_tag": "ALL",  # 如果是飞天job，填写AY54或者AY500
                    "except_ret": 0,  # 0
               }
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.update_tasks(
                owner_id,
                task,
                old_task,
                template)
        new_task_log = ("new_task:{%s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s}" % (
                task.pl_id, task.pid, task.next_task_ids,
                task.prev_task_ids, task.over_time, task.name, task.config,
                task.retry_count, task.last_run_time, task.priority,
                task.except_ret, task.description, task.server_tag, template))
        old_task_log = ''
        if old_task is not None:
            old_task_log = ("old_task:{%s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s, %s}" % (
                    old_task.pl_id, old_task.pid, old_task.next_task_ids,
                    old_task.prev_task_ids, old_task.over_time,
                    old_task.name, old_task.config,
                    old_task.retry_count, old_task.last_run_time,
                    old_task.priority,
                    old_task.except_ret, old_task.description))
        use_time = time.time() - beign_time
        self.__log.info("update_tasks owner_id: %s, "
                "%s %s ret: %s[use_time:%s]"
                % (owner_id, new_task_log, old_task_log, ret, use_time))
        return ret

    def get_task_info(self, task_id):
        """
        通过task_id获取任务详情

        Args:
            task_id: int task id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "task":
                {
                    "id": 1,
                    "pl_id": 1,
                    "pid": 1,
                    "next_task_ids": "2,3",
                    "prev_task_ids": "4,",
                    "over_time": 12,
                    "name": "test_task",
                    "config": "input_file = oss://AY500/test/test.* \n
                                output_file = oss://AY500/test/test.log",
                    "retry_count": 3,
                    "last_run_time": "20150516100000",
                    "description": "test",
                    "priority": 6,
                    "server_tag": "ALL",  # 如果是飞天job，填写AY54或者AY500
                    "except_ret": 0,  # 0
               }
            }
        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_task_info(task_id)
        use_time = time.time() - beign_time
        self.__log.info("get_task_info task_id: %s, ret: %s[use_time:%s]"
                 % (task_id, ret, use_time))
        return ret

    def get_processor_info(self, processor_id):
        """
        从horae_processor中获取算子信息

        Args:
            processor_id: int processor_id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "processor":
                {
                    "id": 1,
                    "name": "test_proc",
                    "type": 1,  # 任务类型 1 python脚本，2 飞天job
                                    3 odps_sql，4 odps_mr， 5 odps_xlib
                    "template": "",
                    "update_time": "2015-05-16 10:00:00",
                    "description": "test",
                    "owner_id": 12,
                    "config": "input_file = oss://AY500/test/test.* \n
                                output_file = oss://AY500/test/test.log",
                    "private": 0,  # 0 公有 1 私有
                    "tag": "a,b,c,d",
                    "tpl_files": "run.json.tpl",
                    "server_tag": "ALL",
                    "user_list": "2,3"  # 拥有读权限的用户
               }
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_processor_info(processor_id)
        use_time = time.time() - beign_time
        self.__log.info("get_proessor_info "
                 "processor_id: %s, ret: %s[use_time:%s]" % (
                 processor_id, ret, use_time))
        return ret

    def get_processor_package_history(self, processor_id):
        """
        通过算子id获取算子的包上传历史

        Args:
            processor_id: int processor_id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "package_historys": [
                    {
                        "id": 1,
                        "upload_time": "2015-06-17 10:11:19",
                        "upload_user_id": 1,
                        "upload_user_name": "test_user",
                        "version": "20150516101010",
                        "status": 0,  # 0 没用 1 使用中
                        "description": "test_description"  # 描述
                   }
                ]
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_processor_package_history(processor_id)
        use_time = time.time() - beign_time
        self.__log.info("get_processor_package_history "
                 "processor_id: %s, ret: %s[use_time:%s]" % (
                 processor_id, ret, use_time))
        return ret

    def get_processor_quote_num(self, processor_id):
        """
        获取算子引用列表数

        Args:
            processor_id: int processor_id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "quote_num": 10
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_processor_quote_num(processor_id)
        use_time = time.time() - beign_time
        self.__log.info("get_processor_quote_num processor_id: %s, "
                 "ret: %s[use_time:%s]"
                 % (processor_id, ret, use_time))
        return ret

    def get_all_authed_pipeline_info(self, owner_id, task_id=None):
        """
        通过owner id 获取所有有读写权限的流程信息

        Args:
            owner_id: int 用户id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "pipelines": [
                    {
                        "id": 0,
                        "name": "test"
                   },
                ]
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_all_authed_pipeline_info(owner_id, task_id)
        use_time = time.time() - beign_time
        self.__log.info("get_all_authed_pipeline_info  owner_id: %s, task_id: %s"
                "ret: %s[use_time:%s]" % (owner_id, task_id, ret, use_time))
        return ret

    def get_processor_tree_async(self, user_id, tree_id):
        """
            获取流程分类树，异步获取

            Args:
                user_id: int 流程原始owner
                tree_id: int 项目树id

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_processor_tree_async(user_id, tree_id)
        use_time = time.time() - beign_time
        self.__log.info("get_project_tree_async user_id: %s, tree_id: %s,"
                        " ret: %s[use_time:%s]"
                        % (user_id, tree_id, ret, use_time))
        return ret

    def get_all_authed_processor(self, owner_id, type):
        """
        通过owner_id获取所有有权限(公有和私有）的算子

        Args:
            owner_id: int owner id
            type: int # 任务类型

        Returns:
            返回一个列表，列表中的元素为一个Processor实例(详见horae/models.py)
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "processors": [
                    {
                        "id": 1,
                        "name": "test_proc",
                    },
                ]
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_all_authed_processor(owner_id, type)
        use_time = time.time() - beign_time
        self.__log.info("get_all_authed_processor "
                 "owner_id: %s, type: %s, ret: %s[use_time:%s]"
                 % (owner_id, type, ret, use_time))
        return ret

    def add_new_task_to_pipeline(self, owner_id, task, processor=None):
        """
        向指定的pipeline中插入一个task

        Args:
            owner_id: int owner_id
            task：一个Task实例(详见horae/models.py)
            processor：一个Processor实例(详见horae/models.py)
                       如果用户选择新建算子，则需要传入这个参数

        Returns:
            返回一个状态，以及一个字符串，
            状态为0表示成功，其他失败
            字符串表示失败原因，成功则为‘OK’
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "task":
                {
                    "id": 1,
                    "pl_id": 1,
                    "pid": 1,
                    "next_task_ids": "2,3",
                    "prev_task_ids": "4,",
                    "over_time": 12,
                    "name": "test_task",
                    "config": "input_file = oss://AY500/test/test.* \n
                                output_file = oss://AY500/test/test.log",
                    "retry_count": 3,
                    "last_run_time": "20150516100000",
                    "description": "test",
                    "priority": 6,
                    "server_tag": "ALL",  # 如果是飞天job，填写AY54或者AY500
                    "except_ret": 0,  # 0
               }
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.add_new_task_to_pipeline(
                owner_id,
                task,
                processor)
        use_time = time.time() - beign_time
        self.__log.info("add_new_task_to_pipeline owner_id: %s, "
                "task:{pl_id: %s, pid: %s, next_task_ids: %s, "
                "prev_task_ids: %s, over_time: %s, name: %s, "
                "config: %s, retry_count: %s, last_run_time: %s, "
                "priority: %s, except_ret: %s, description: %s} "
                "server_tag: %s, ret: %s[use_time:%s]"
                % (owner_id, task.pl_id, task.pid, task.next_task_ids,
                task.prev_task_ids, task.over_time, task.name, task.config,
                task.retry_count, task.last_run_time, task.priority,
                task.except_ret, task.description,
                task.server_tag, ret, use_time))
        return ret

    def create_new_pipeline(
            self,
            name,
            ct_time,
            owner_id,
            manager_id_list,
            monitor_way,
            tag,
            description,
            life_cycle=None,
            type=0,
            project_id=0):
        """
        向horae_pipeline新建一个pipeline

        Args:
            name: string pipeline名字
            ct_time: string 调度时间
            owner_id：int 创建者id
            manager_id_list: int 拥有管理权限的用户id列表
            monitor_way: int 报警方式 0邮件，1钉钉，2二者都报
            tag: string 标签, 多个标签','分隔
            description: string 描述
            life_cycle: int 生命周期，单位天
            type: int 类型
            project_id: int 项目id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.create_new_pipeline(
                name,
                ct_time,
                owner_id,
                manager_id_list,
                monitor_way,
                tag,
                description,
                life_cycle,
                type,
                project_id)
        use_time = time.time() - beign_time
        self.__log.info("create_new_pipeline owner_id: %s, "
                "name: %s, ct_time: %s, manager_id_list: %s,"
                " monitor_way: %s, tag: %s, description:%s,"
                " life_cycle: %s, project_id: %s, "
                " ret: %s[use_time:%s]" % (
                owner_id, name, ct_time, str(manager_id_list),
                monitor_way, tag, description, life_cycle,
                project_id, ret, use_time))
        return ret

    def get_all_user_info(self):
        """
        获取所有用户信息

        Args:
            No.

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "users": [
                    {
                        "id": 0,
                        "username": "test",
                   },
                ]
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_all_user_info()
        use_time = time.time() - beign_time
        self.__log.info("get_all_user_info  ret: %s[use_time:%s]" % (
                ret, use_time))
        return ret

    def get_pipeline_with_project_tree(self, user_id, id):
        """
            得到一个流程及其分类树

            Args:
                user_id: int 流程原始owner
                id： int 流程id

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_pipeline_with_project_tree(user_id, id)
        use_time = time.time() - beign_time
        self.__log.info("get_pipeline_with_project_tree user_id: %s, id: %s,"
                        " ret: %s[use_time:%s]"
                        % (user_id, id, ret, use_time))
        return ret

    def search_processor(self, user_id, word, with_project=1, limit=100):
        """
            搜索算子信息

            Args:
                user_id: int 流程原始owner
                word: string 搜索关键词
                with_project: int 0只搜索算子， 1 附带算子的所有分类信息并展开
                limit: int 搜索条数

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.search_processor(user_id, word, with_project, limit)
        use_time = time.time() - beign_time
        self.__log.info("search_processor user_id: %s, word: %s, ret: %s[use_time:%s]"
                        % (user_id, word, ret, use_time))
        return ret

    def get_processor_quote_list(self, processor_id):
        """
        获取算子引用列表

        Args:
            processor_id: int processor_id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "quote_list": [
                    {
                        "pipeline_id": 1,
                        "pipeline_name": "test_pipe",
                        "task_id": 1,
                        "task_name": "test_name",
                        "owner_id": 1,
                        "owner_name": "test_user",
                        "last_run_time": "201507151000",
                   }
                ]
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_processor_quote_list(processor_id)
        use_time = time.time() - beign_time
        self.__log.info("get_processor_quote_list processor_id: %s, "
                 "ret: %s[use_time:%s]"
                 % (processor_id, ret, use_time))
        return ret

    def update_processor(self, user_id, processor, user_id_list=None):
        """
        修改一个算子

        Args:
            user_id: 一个Processor实例，详见（horae/models.py）
            processor: 一个Processor实例，详见（horae/models.py）
            user_id_list: 具有使用权限的用户id列表，','分隔

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.update_processor(
                user_id,
                processor,
                user_id_list)
        use_time = time.time() - beign_time
        self.__log.info("update_processor "
                 "name: %s, type: %s, "
                 "template: %s, update_time: %s, "
                 "description: %s, config: %s, owner_id: %s, "
                 "private: %s, tag: %s, tpl_files: %s, user_id_list: %s "
                 "ret: %s[use_time:%s]"
                 % (processor.name, processor.type,
                 processor.template,
                 processor.update_time, processor.description,
                 processor.config, processor.owner_id,
                 processor.private, processor.tag, processor.tpl_files,
                 user_id_list, ret, use_time))
        return ret

    # 执行状态相关接口
    def show_run_history(
            self,
            show_tag,
            owner_id,
            page_min,
            page_max,
            order_field,
            sort_order,
            search_map=None,
            search_list=None):
        """
        查看任务
        从horae_runhistory中获取执行状态信息，支持排序，翻页，搜索功能

        Args:
            show_tag: int 0 展示任务历史 1 展示流程历史
            owner_id: int 用户id
            page_min: int 翻页最小索引
            page_max：int 翻页最大索引
            order_field: string 排序字段
            sort_order: string 排序方式 'desc'降序 'asc'升序
            search_map: map 需要搜索的字段，
                            每一个key表示需要搜索的字段名，
                            每一个对应value表示搜索关键字，
                            value前缀可以加=,>=,<=，>,<,!=比较操作符
                            如果有比较操作符，且是字符串，则需在字符串前后加''
            search_list: list [(key, value), (key, value)]
                            key,value意义同search_map

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "count": 10,  # 总数
                "runhistory_list": [
                    {
                        "id": 1,  # run_history 的id
                        "task_id": 1,  # 任务id
                        "schedule_id": 1,  # schedule id
                        "run_time": "201506151010",  # run_time
                        "pl_id": 1,  # pipeline_id
                        "pl_name": "test_pipeline",  # 流程名
                        "task_name": "test_task",  # 任务名
                        "start_time": "2015-05-16 10:00:09",  # 开始时间
                        "use_time": 4,  # 执行时间 单位：秒
                        "status", 3,  # 参见TaskState,
                        "cpu": 100,  # 使用cpu情况
                        "mem": 100  # 使用内存情况
                    },
                ]
            }
            列表长度最大为page_max - page_min，
            并按照order_field排序

        Raises:
            No.
        """
        beign_time = time.time()
        if search_list is None:
            search_list = []

        if search_map is not None:
            for key in search_map:
                search_list.append((key, search_map[key]))
        ret = self.__run_history.show_run_history(
                show_tag,
                owner_id,
                page_min,
                page_max,
                order_field,
                sort_order,
                search_list)
        use_time = time.time() - beign_time
        self.__log.info("show_run_history "
                 "show_tag： %s, owner_id: %s, page_min: %s, page_max: %s, "
                 "order_field: %s, sort_order: %s, search_map:"
                 " %s, ret: %s[use_time:%s]"
                 % (show_tag, owner_id, page_min, page_max, order_field,
                 sort_order, str(search_list), ret, use_time))
        return ret

    def get_run_history_info(self, run_history_id):
        """
        通过run_historu
        从horae_runhistory中获取执行状态信息，支持排序，翻页，搜索功能

        Args:
            run_history_id: int 执行状态id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "count": 10,  # 总数
                "runhistory": {
                    "task_id": 1,  # 任务id
                    "schedule_id": 1,  # schedule id
                    "run_time": "201506151010",  # run_time
                    "pl_id": 1,  # pipeline_id
                    "start_time": "2015-05-16 10:00:09",  # 开始时间
                    "end_time": "2015-05-16 10:00:09",  # 结束时间
                    "use_time": 4,  # 执行时间 单位：分钟
                    "status", 3,  # 参见TaskState
                    "pl_name", 'test',  # 流程名
                    "task_name", 'test',  # 任务名
                },
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__run_history.get_run_history_info(run_history_id)
        use_time = time.time() - beign_time
        self.__log.info("get_run_history_info "
                 "run_history_id: %s, ret: %s[use_time:%s]"
                 % (run_history_id, ret, use_time))
        return ret

    def run_tasks(self, owner_id, task_id_list, run_time_str, ordered=0):
        """
        用户手动启动任务，要么全部成功，要么全部失败

        Args:
            owner_id: int 操作用户id
            task_id_list: int 任务id列表
            run_time_str: string run_time的组合，
                支持单个run_time 比如：2015061610
                支持多个run_time 比如：2015061610，2015061611
                                 或者区间：2015061610-2015071610
                支持天级别，小时级别，分钟级别
            ordered: 用户提交串行执行 <= 0, 默认执行方式，>0 串行并发ordered流程执行

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.run_tasks(
                owner_id,
                task_id_list,
                run_time_str,
                ordered)
        use_time = time.time() - beign_time
        self.__log.info("run_tasks owner_id: %s, "
                "task_id_list: %s, run_time_str: %s, "
                "ordered: %s ret: %s [use_time:%s]" % (
                owner_id, str(task_id_list), run_time_str,
                ordered, ret, use_time))
        return ret

    def create_processor(self, processor, user_id_list=None):
        """
        创建一个算子

        Args:
            processor: 一个Processor实例，详见（horae/models.py）
            user_id_list: 具有使用权限的用户id列表，','分隔

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.create_processor(processor, user_id_list)
        use_time = time.time() - beign_time
        self.__log.info("create_processor "
                 "name: %s, type: %s, "
                 "template: %s, update_time: %s, "
                 "description: %s, config: %s, owner_id: %s, "
                 "private: %s, tag: %s, tpl_files: %s, user_id_list: %s"
                 "ret: %s[use_time:%s]"
                 % (processor.name, processor.type,
                 processor.template,
                 processor.update_time, processor.description,
                 processor.config, processor.owner_id,
                 processor.private, processor.tag, processor.tpl_files,
                 user_id_list, ret, use_time))
        return ret

    def get_proc_project_tree(self, user_id, type=-1):
        """
            获取算子分类树

            Args:
                user_id: int 流程原始owner
                type: int -1 用户自己创建的 其他值无效

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_proc_project_tree(user_id, type)
        use_time = time.time() - beign_time
        self.__log.info("get_proc_project_tree user_id: %s, type: %s, ret: %s[use_time:%s]"
                        % (user_id, type, ret, use_time))
        return ret

    def get_shared_processor_tree_async(self, user_id, tree_id):
        """
            获取共享给用户的算子，包括子分类

            Args:
                user_id: int 流程原始owner
                tree_id: int 分类id

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_shared_processor_tree_async(user_id, tree_id)
        use_time = time.time() - beign_time
        self.__log.info("get_shared_processor_tree_async user_id: %s,"
                        " tree_id: %s, ret: %s[use_time:%s]"
                        % (user_id, tree_id, ret, use_time))
        return ret

    def get_standard_processor_tree_async(self, user_id, tree_id):
        """
            获取共享给用户的算子，包括子分类

            Args:
                user_id: int 流程原始owner
                tree_id: int 分类id

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_standard_processor_tree_async(user_id, tree_id)
        use_time = time.time() - beign_time
        self.__log.info("get_standard_processor_tree_async user_id: %s,"
                        " tree_id: %s, ret: %s[use_time:%s]"
                        % (user_id, tree_id, ret, use_time))
        return ret

    def add_new_project(
            self,
            owner_id,
            project_name,
            writer_list,
            description='',
            parent_id=0,
            type=0):
        """
        创建新项目接口

        Args:
            owner_id: int 用户id
            project_name： string 项目名
            writer_list：string 拥有管理权限的用户id列表，
                                比如"1,2,3"，多个id之间','分隔
            description： string 描述

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.add_new_project(
            owner_id,
            project_name,
            writer_list,
            description,
            parent_id,
            type)
        use_time = time.time() - beign_time
        self.__log.info("add_new_project owner_id: %s, project_name: %s"
                        "writer_list: %s, description: %s, parent_id: %s, ret: %s[use_time:%s]"
                        % (owner_id, project_name,
                           writer_list, description, parent_id, ret, use_time))
        return ret

    def public_processor(self, processor_id, project_id):
        """
        将算子公有

        Args:
            processor_id: int processor_id
            project_id: int 算子放入分类id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.public_processor(processor_id, project_id)
        use_time = time.time() - beign_time
        self.__log.info("public_processor processor_id: %s, "
                 "project_id: %s, ret: %s[use_time:%s]"
                 % (processor_id, project_id, ret, use_time))
        return ret

    def upload_package_with_local(self, user_id, proc_id, version_name,
                                  desc, git_url=None, file_name=None, type=-1):
        """
            搜索流程信息

            Args:
                user_id: int 流程原始owner
                proc_id: int 算子id
                version_name： string 算子版本名
                desc： string 版本描述
                git_url： string 如果不为none，则通过git创建算子版本
                file_name： string 如果不为None，则通过本地文件创建算子版本

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.upload_package_with_local(
            user_id, proc_id, version_name, desc, git_url, file_name, type)
        use_time = time.time() - beign_time
        self.__log.info("upload_package user_id: %s, proc_id: %s,"
                        " version_name: %s, desc: %s, "
                        "git_url: %s, file_name: %s, ret: %s[use_time:%s]"
                        % (user_id, proc_id, version_name, desc,
                           git_url, file_name, ret, use_time))
        return ret

    def delete_task_info(self, owner_id, task_id):
        """
        删除一个task

        Args:
            owner_id: int owner_id
            task_id：int task id

        Returns:
            返回一个状态，以及一个字符串，
            状态为0表示成功，其他失败
            字符串表示失败原因，成功则为‘OK’
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.delete_task_info(owner_id, task_id)
        use_time = time.time() - beign_time
        self.__log.info("delete_task_info owner_id: %s, "
                "task_id: %s, ret: %s[use_time:%s]"
                 % (owner_id, task_id, ret, use_time))
        return ret

    def get_proc_with_project_tree(self, user_id, id):
        """
            得到一个算子及其分类树

            Args:
                user_id: int 流程原始owner
                id： int 算子id

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.get_proc_with_project_tree(user_id, id)
        use_time = time.time() - beign_time
        self.__log.info("get_proc_with_project_tree user_id: %s, id: %s,"
                        " ret: %s[use_time:%s]"
                        % (user_id, id, ret, use_time))
        return ret

    def delete_processor(self, user_id, processor_id):
        """
        删除一个算子

        Args:
            user_id: int 用户id
            processor_id: int processor_id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.delete_processor(user_id, processor_id)
        use_time = time.time() - beign_time
        self.__log.info("delete_processor "
                 "user_id: %s, processor_id: %s, ret: %s[use_time:%s]"
                 % (user_id, processor_id, ret, use_time))
        return ret

    def delete_proc_version(self, user_id, proc_id, id):
        """
            删除一个算子版本

            Args:
                user_id: int 流程原始owner
                proc_id: int 算子id
                id： int 算子版本id

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.delete_proc_version(user_id, proc_id, id)
        use_time = time.time() - beign_time
        self.__log.info("delete_proc_version user_id: %s, id: %s,"
                        " ret: %s[use_time:%s]"
                        % (user_id, id, ret, use_time))
        return ret

    def delete_project(self, owner_id, project_id):
        """
        删除项目接口

        Args:
            owner_id: int 用户id
            project_id： int 项目id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.delete_project(owner_id, project_id)
        use_time = time.time() - beign_time
        self.__log.info(
                "delete_project owner_id: %s, project_id: %s, "
                "ret: %s[use_time:%s]"
                % (owner_id, project_id, ret, use_time))
        return ret

    def get_task_run_status_info(self, task_id, run_time):
        """
        获取等待中的任务上游信息

        Args:
            task_id: int 任务 id
            run_time: string 执行时间

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "prev_info": {  # 如果数据不为空，则表示其依赖其他pipeline
                                  且有未完成任务,
                                  否则表示没有上游或者其上游依赖全部成功
                    "3": [  # 详见TaskState
                        {
                            "task_id": 99,
                            "start_time": "2015-06-15 10:00:00",
                            "pipeline_name": "run_his94",
                            "task_name": "run_his_task_99",
                            "run_time": "201507061310",
                            "pl_id": 94,
                            "id": 6,
                            "manager_list": [
                                (username, email),
                                (username, email)
                            ]
                        }
                    ]
                    "1": [  # 详见TaskState
                        {
                            "task_id": 89,
                            "start_time": "2015-06-15 10:00:00",
                            "pipeline_name": "run_his94",
                            "task_name": "run_his_task_89",
                            "run_time": "201507061310",
                            "pl_id": 94,
                            "id": 6
                            "manager_list": [
                                (username, email),
                                (username, email)
                            ]
                        }
                    ]
                }
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = ''
        try:
            ret = self.__run_history.get_task_run_status_info(task_id, run_time)
        except Exception as ex:
            error_log = ("run_history_show_graph fail![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            self.__log.error(error_log)
            ret = self.__get_default_ret_map(1, error_log)

        use_time = time.time() - beign_time
        self.__log.info("get_task_run_status_info "
                 "task_id: %s, task_id: %s, ret: %s[use_time:%s]"
                 % (task_id, run_time, ret, use_time))
        return ret

    def __get_default_ret_map(self, status, info):
        ret_map = {}
        ret_map["status"] = status
        ret_map["info"] = info
        return json.dumps(ret_map)

    def run_history_show_graph(self, pipeline_id, run_time):
        """
        通过pipeline id获取这个流程的所有任务数据，
        同时获取上游（其他pipeline）依赖task任务状态信息

        Args:
            pipeline_id: int pipeline id
            run_time: string 执行时间

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "runhistory_list": [
                    {
                        "task_id": 1,  # 任务id
                        "schedule_id": 1,  # schedule id
                        "run_time": "201506151010",  # run_time
                        "pl_id": 1,  # pipeline_id
                        "pl_name": "test_pipeline",  # 流程名
                        "task_name": "test_task",  # 任务名
                        "begin_time": "2015-05-16 10:00:09",  # 开始时间
                        "status", 3,  # 参见TaskState
                        "next_task_ids": "2,3",
                        "prev_task_ids": "4,",
                        "retried_count": 3,
                        "un_succ_run_tasks": [  # 如果数据不为空，
                                          则表示其有未完成任务
                            {
                                "status": 3,
                                "run_time": "201507061310",
                            }
                        ]
                        "manager_list": [
                            (username, email),
                            (username, email)
                        ]
                    }
                ]
            }

        Raises:
            No.
        """
        beign_time = time.time()
        try:
            ret = self.__run_history.run_history_show_graph(
                    pipeline_id,
                    run_time)
        except Exception as ex:
            error_log = ("run_history_show_graph fail![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            self.__log.error(error_log)
            return self.__get_default_ret_map(1, error_log)

        use_time = time.time() - beign_time
        self.__log.info("run_history_show_graph "
                 "pipeline_id： %s, run_time: %s, ret: %s[use_time:%s]"
                 % (pipeline_id, run_time, ret, use_time))
        return ret

    def get_task_run_logs(self, schedule_id, sub_path=None, rerun_id=0):
        """get_task_run_logs
        获取任务执行的日志文件列表

        Args:
            schedule_id: int 任务schedule id
            sub_path: string 子文件路径（比如 前后不加 /)
            rerun_id: int 任务重试id

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "log_file_list": "stdout.log\nstderr.log\nn_run.json"
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_task_run_logs(schedule_id, sub_path, rerun_id)
        use_time = time.time() - beign_time
        self.__log.info("get_task_run_logs "
                 "schedule_id： %s, ret: %s[use_time:%s]"
                 % (schedule_id, ret, use_time))
        return ret

    def get_task_log_content(self, schedule_id, file_name, file_offset, len, rerun_id=0):
        """
        获取日志内容
    
        Args:
            schedule_id: int 任务schedule id
            file_name：string 文件名, 可附加子文件夹比如 log/log.log
            file_offset: int 文件偏移位置
            len：int 通过file_offset获取len长度的内容
    
        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "file_content": "sdfadsfadf"
            }
    
        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_task_log_content(
            schedule_id,
            file_name,
            file_offset,
            len,
            rerun_id)
        use_time = time.time() - beign_time
        self.__log.info("get_task_log_content "
                        "schedule_id： %s, file_name: %s, file_offset: %s, "
                        "len: %s, ret: %s[use_time:%s]"
                        % (schedule_id, file_name, file_offset, len, 'test', use_time))
        return ret

    def get_log_content_tail(self, schedule_id, file_name, rerun_id):
        """
        获取日志内容尾部几行内容
    
        Args:
            schedule_id: int 任务schedule id
            file_name：string 文件名, 可附加子文件夹比如 log/log.log
    
        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "file_content": "sdfadsfadf"
            }
    
        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_log_content_tail(
            schedule_id,
            file_name,
            rerun_id)
        use_time = time.time() - beign_time
        self.__log.info("get_log_content_tail "
                        "schedule_id： %s, file_name: %s, ret: %s[use_time:%s]"
                        % (schedule_id, file_name, ret, use_time))
        return ret

    def run_pipeline(self, owner_id, pipeline_id, run_time_str, ordered_num=0):
        """
        用户手动启动整个pipeline，要么全部成功，要么全部失败

        Args:
            owner_id: int 操作用户id
            pipeline_id: int 需要启动的pipeline id
            run_time_str: string run_time的组合，
                支持单个run_time 比如：2015061610
                支持多个run_time 比如：2015061610，2015061611
                                 或者区间：2015061610-2015071610
                支持天级别，小时级别，分钟级别
            ordered_num: 并发数

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.run_pipeline(
                owner_id,
                pipeline_id,
                run_time_str,
                ordered_num)
        use_time = time.time() - beign_time
        self.__log.info("run_pipeline owner_id: %s, "
                "pipeline_id: %s, run_time_str: %s, ordered_num: %s, ret: %s[use_time:%s]" % (
                owner_id, pipeline_id, run_time_str, ordered_num, ret, use_time))
        return ret

    def stop_task(self, owner_id, task_id, run_time):
        """
        用户停止任务，一个task_id对应一个run_time
    
        Args:
            owner_id: int 操作用户id
            task_id: int 任务id
            run_time： string 执行时间
    
        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }
    
        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.stop_task(
            owner_id,
            task_id,
            run_time)
        use_time = time.time() - beign_time
        self.__log.info("stop_task owner_id: %s, "
                        "task_id: %s, run_time: %s, ret: %s[use_time:%s]" % (
                            owner_id, task_id, run_time, ret, use_time))
        return ret

    def run_task_with_all_successors(self, owner_id, task_id, run_time):
        """
        用户手动启动task以及其下游依赖task（不跨越pipeline）
    
        Args:
            owner_id: int 操作用户id
            task_id: int 需要启动的task id
            run_time: string 只支持单个run_time 比如：2015061610
    
        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }
    
        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.run_task_with_all_successors(
            owner_id,
            task_id,
            run_time)
        use_time = time.time() - beign_time
        self.__log.info("run_task_with_all_successors owner_id: %s, "
                        "task_id: %s, run_time: %s ret: %s[use_time:%s]" % (
                            owner_id, task_id, run_time, ret, use_time))
        return ret

    def set_task_success(self, user_id, task_id, run_time):
        """
            设置任务状态为成功

            Args:
                user_id: int 操作用户id
                task_id: int 需要启动的task id
                run_time: string 只支持单个run_time 比如：2015061610

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.set_task_success(
            user_id,
            task_id,
            run_time)
        use_time = time.time() - beign_time
        self.__log.info("set_task_success owner_id: %s, "
                        "task_id: %s, run_time: %s ret: %s[use_time:%s]" % (
                        user_id, task_id, run_time, ret, use_time))
        return ret

    def run_one_by_one_task(self, owner_id, task_pair_list, ordered_num=0):
        """
            用户手动一对一的启动任务，一个task_id对应一个run_time
            要么全部成功，要么全部失败

            Args:
                owner_id: int 操作用户id
                task_pair_list: [(task_id, run_time), (task_id, run_time)]
                ordered_num: int 并发数 为0则并行，>0 串行执行

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.run_one_by_one_task(
                owner_id,
                task_pair_list,
                ordered_num)
        use_time = time.time() - beign_time
        self.__log.info("run_one_by_one_task owner_id: %s, "
                "task_pair_list: %s, ordered_num: %s, ret: %s[use_time:%s]" % (
                owner_id, str(task_pair_list), ordered_num, ret, use_time))
        return ret

    def get_retry_history_list(self, user_id, schedule_id):
        """
            用户手动一对一的启动任务，一个task_id对应一个run_time
            要么全部成功，要么全部失败

            Args:
                owner_id: int 操作用户id
                task_pair_list: [(task_id, run_time), (task_id, run_time)]
                ordered_num: int 并发数 为0则并行，>0 串行执行

            Returns:
                {
                    "status": 0,  # 0 成功，其他失败
                    "info": "OK",  # 失败原因
                }

            Raises:
                No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.get_retry_history_list(user_id, schedule_id)
        use_time = time.time() - beign_time
        self.__log.info("get_retry_history_list user_id: %s, "
                        "schedule_id: %s, ret: %s[use_time:%s]" % (
                        user_id, schedule_id, ret, use_time))
        return ret

    def pipeline_off_or_on_line(self, owner_id, pipeline_id, on_line, reason):
        """
        让一个流程上下线

        Args:
            owner_id: int 操作用户id
            pipeline_id: int pipeline_id
            on_line: int 0 下线， 1 上线

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.pipeline_off_or_on_line(
                owner_id,
                pipeline_id,
                on_line,
                reason)
        use_time = time.time() - beign_time
        self.__log.info("pipeline_off_or_on_line owner_id: %s, "
                "pipeline_id: %s, on_line: %s ret: %s[use_time:%s]" % (
                owner_id, pipeline_id, on_line, ret, use_time))
        return ret

    def upload_pacakge(
            self,
            processor_id,
            owner_id,
            file_path,
            version_name,
            description=None,
            overwrite=False):
        """
        上传算子成功后调用接口

        Args:
            processor_id: int 算子id
            owner_id: int 用户id
            file_path： string 算子包路径
            version_name: string 版本名
            description： string 描述

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__processor_mgr.upload_pacakge(
                processor_id,
                owner_id,
                file_path,
                version_name,
                description,
                overwrite)
        use_time = time.time() - beign_time
        self.__log.info("upload_pacakge "
                 "processor_id: %s, owner_id: %s, file_path: %s, "
                 "version_name: %s, description: %s "
                 "ret: %s[use_time:%s]"
                 % (processor_id, owner_id, file_path,
                    version_name, description, ret, use_time))
        return ret

    def copy_pipeline(
            self,
            owner_id,
            src_pl_id,
            new_pl_name,
            project_id,
            use_type_src=False):
        """
        拷贝DAG流

        Args:
            owner_id: int 用户id
            src_pl_id： int 被拷贝DAG流id
            new_pl_name：string 新DAG流名
            project_id：int 项目id
            use_type_src: bool TrueDAG流类型使用被拷贝DAG流的类型,False使用0

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "pl_id": 1,  # 新建的DAG流id
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.copy_pipeline(
                owner_id,
                src_pl_id,
                new_pl_name,
                project_id,
                use_type_src)
        use_time = time.time() - beign_time
        self.__log.info("copy_pipeline owner_id: %s, src_pl_id: %s, "
                 "new_pl_name: %s, use_type_src: %s, ret: %s[use_time:%s]"
                 % (owner_id, src_pl_id, new_pl_name,
                 use_type_src, ret, use_time))
        return ret

    def copy_task(self, owner_id, src_task_id, dest_pl_id):
        """
        拷贝任务

        Args:
            owner_id: int 用户id
            src_pl_id： int 被拷贝DAG流id
            dest_pl_id: int 拷贝到的目标DAG流

        Returns:
            {
                "status": 0,  # 0 成功，其他失败
                "info": "OK",  # 失败原因
                "task":
                {
                    "id": 1,
                    "pl_id": 1,
                    "pid": 1,
                    "next_task_ids": "2,3",
                    "prev_task_ids": "4,",
                    "over_time": 12,
                    "name": "test_task",
                    "retry_count": 3,
                    "last_run_time": "20150516100000",
                    "description": "test",
                    "priority": 6,
                    "server_tag": "ALL",  # 如果是飞天job，填写AY54或者AY500
                    "except_ret": 0,  # 0
               }
            }

        Raises:
            No.
        """
        beign_time = time.time()
        ret = self.__pipeline_mgr.copy_task(
                owner_id,
                src_task_id,
                dest_pl_id)
        use_time = time.time() - beign_time
        self.__log.info("copy_task owner_id: %s, src_task_id: %s, "
                 "dest_pl_id: %s, ret: %s[use_time:%s]"
                 % (owner_id, src_task_id, dest_pl_id, ret, use_time))
        return ret
