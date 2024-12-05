###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import threading
import random
import logging

sys.path.append('../common')
import task_util

class TaskNodeManager():
    """
        管理tasknode信息
        1.监控zookeeper相关路径，获取task_node信息
        2.提供给task有效的机器ip
    """
    def __init__(self, config, zk_manager):
        self.__log = logging
        self.__mutex = threading.Lock()
        self.__type_task_node_map = {}
        self.__type_task_node_index = {}
        self.__zk_manager = zk_manager
        self.__task_type_conf_name = (
            'script',
            'spark',
            'oozie',
            'odps',
            'shell',
            'docker',
            'clickhouse',
            'v100',
            'local_docker',
        )

        self.__type_name_id_map = {
            'script': task_util.TaskType.SCRIPT_TYPE,
            'spark': task_util.TaskType.SPARK_TYPE,
            'oozie': task_util.TaskType.OOZIE_TYPE,
            'odps': task_util.TaskType.ODPS_TYPE,
            'shell': task_util.TaskType.SHELL_TYPE,
            'docker': task_util.TaskType.DOCKER_TYPE,
            'clickhouse': task_util.TaskType.CLICKHOUSE_TYPE,
            'v100': task_util.TaskType.V100_TYPE,
            'local_docker': task_util.TaskType.LOCAL_DOCKER_TYPE,
        }

        self.__type_name_watch_func_map = {
            'script': self.__watch_script_task_node_children,
            'spark': self.__watch_spark_task_node_children,
            'oozie': self.__watch_oozie_task_node_children,
            'odps': self.__watch_odps_task_node_children,
            'shell': self.__watch_shell_task_node_children,
            'docker': self.__watch_docker_task_node_children,
            'clickhouse': self.__watch_clickhouse_task_node_children,
            'v100': self.__watch_v100_task_node_children,
            'local_docker': self.__watch_local_docker_task_node_children,
        }

        self.__get_path_info_from_conf(config)

    def get_valid_ip(self, task_type, server_tag):
        # task_util.AutoLock(self.__mutex)
        self.__mutex.acquire()
        try:
            if len(self.__type_task_node_map[task_type][server_tag]) <= 0:
                return None

            if task_type not in self.__type_task_node_index:
                self.__type_task_node_index[task_type] = {}

            if server_tag not in self.__type_task_node_index[task_type]:
                self.__type_task_node_index[task_type][server_tag] = 0

            index = self.__type_task_node_index[task_type][server_tag]
            self.__type_task_node_index[task_type][server_tag] = self.__type_task_node_index[task_type][server_tag] + 1
            if self.__type_task_node_index[task_type][server_tag] >= len(self.__type_task_node_map[task_type][server_tag]):
                self.__type_task_node_index[task_type][server_tag] = 0

            # random_ip = random.randint(
            #         0,
            #         len(self.__type_task_node_map[task_type][server_tag]) - 1)
            return self.__type_task_node_map[task_type][server_tag][index]
        except KeyError as ex:
            self.__log.error(str(ex) + "type_task_node_map wrong!")
            return None
        except Exception as ex:
            self.__log.error(str(ex) + "type_task_node_map wrong!")
            return None
        finally:
            self.__mutex.release()

    def __handle_watch(self, children, type):
        # task_util.AutoLock(self.__mutex)
        self.__mutex.acquire()
        if children is None or len(children) <= 0:
            if type in self.__type_task_node_map:
                del self.__type_task_node_map[type]
            self.__mutex.release()
            return

        if type in self.__type_task_node_map:
            self.__type_task_node_map[type] = {}

        for child in children:
            child_split = child.split(":")
            if len(child_split) != 2:
                self.__log.error(
                        "type:%d zookeeper path wrong child![%s]", 
                        type, child)
                continue
            if type in self.__type_task_node_map:
                if child_split[0] in self.__type_task_node_map[type]:
                    self.__type_task_node_map[type][child_split[0]].append(
                            child_split[1])
                else:
                    ip_list = []
                    ip_list.append(child_split[1])
                    tmp_map = {}
                    self.__type_task_node_map[type][child_split[0]] = ip_list
            else:
                ip_list = []
                ip_list.append(child_split[1])
                tmp_map = {}
                tmp_map[child_split[0]] = ip_list
                self.__type_task_node_map[type] = tmp_map
        self.__mutex.release()

    def __watch_script_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.SCRIPT_TYPE)

    def __watch_spark_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.SPARK_TYPE)

    def __watch_oozie_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.OOZIE_TYPE)

    def __watch_odps_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.ODPS_TYPE)

    def __watch_shell_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.SHELL_TYPE)

    def __watch_docker_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.DOCKER_TYPE)

    def __watch_clickhouse_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.CLICKHOUSE_TYPE)

    def __watch_v100_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.V100_TYPE)

    def __watch_local_docker_task_node_children(self, children):
        self.__handle_watch(children, task_util.TaskType.LOCAL_DOCKER_TYPE)

    def __get_path_info_from_conf(self, config):
        for task_type_name in self.__task_type_conf_name:
            zk_path = config.get("zk", task_type_name)
            self.__handle_watch(
                    self.__zk_manager.get_children(zk_path), 
                    self.__type_name_id_map[task_type_name])
            self.__zk_manager.watch_children(
                    zk_path, 
                    self.__type_name_watch_func_map[task_type_name])
