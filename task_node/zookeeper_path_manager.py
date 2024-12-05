###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import threading
import time
import logging

sys.path.append('../common')
import task_util

class ZooKeeperPathManager(threading.Thread):
    """
        管理tasknode信息
        1.task node 启动的时候注册本地信息，包括可以处理的任务类型，ip
        2.维护与zookeeper的临时节点信息
    """
    def __init__(self, config, zk_manager):
        threading.Thread.__init__(self)
        self.__log = logging
        self.__config = config
        self.__type_task_node_map = {}
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

        self.__path_list = []
        self.__get_path_info_from_conf(config)

    def run(self):
        self.__log.info("ZooKeeperPathManager thread starting...")
        # 全局退出标示字段
        # 检查临时节点是否被异常删除了，删除了就重新建立
        while not task_util.CONSTANTS.GLOBAL_STOP:
            for child_path in self.__path_list:
                if self.__zk_manager.exists(child_path) is None:
                    if self.__zk_manager.create(
                            path=child_path, 
                            ephemeral=True) is None:
                        self.__log.error("create zookeeper"
                                "ephemeral node[%s] failed!" % child_path)
            time.sleep(10)

        # 线程退出则立即清除临时节点信息
        for child_path in self.__path_list:
            if self.__zk_manager.exists(child_path) is not None:
                if self.__zk_manager.delete(path = child_path) is None:
                    self.__log.error("delete zookeeper"
                            "ephemeral node[%s] failed!" % child_path)

        self.__log.info("ZooKeeperPathManager thread exited!")

    def __handle_zk_path(self, log_path):
        if log_path is None or log_path.strip() == '':
            self.__log.error("path is null or error!")
            return False

        split_arr = log_path.split(":")
        if len(split_arr) != 2:
            self.__log.error("path[%s] is null or error!" % log_path)
            return False

        zk_path = split_arr[0]
        if not self.__zk_manager.exists(zk_path):
            if not self.__zk_manager.create(zk_path):
                self.__log.error("paht[%s] has't created on zookeeper!" % zk_path)
                return False

        task_tag_list = split_arr[1].split(",")
        if task_tag_list is None or len(task_tag_list) <= 0:
            self.__log.error("path[%s] is null or error!" % log_path)
            return False

        # 不处理此种类型的任务
        if len(task_tag_list) == 1 and task_tag_list[0] == "None":
            return True

        local_public_ip = self.__config.get("node", "public_ip")
        if local_public_ip is None or local_public_ip.strip() == "":
            local_public_ip = task_util.StaticFunction.get_local_ip()
        # local_ip = task_util.StaticFunction.get_local_ip()
        for task_tag in task_tag_list:
            child_path = zk_path + "/" + task_tag + ":" + local_public_ip
            self.__path_list.append(child_path)
            self.__zk_manager.create(path=child_path, ephemeral=True)
        return True

    def __get_path_info_from_conf(self, config):
        for task_type_name in self.__task_type_conf_name:
            if config.has_option("zk", task_type_name):
                zk_path = config.get("zk", task_type_name)
                self.__log.info("handle zk path " + zk_path)
                if not self.__handle_zk_path(zk_path):
                    raise ValueError(
                        "config get node task zookeeper info failed![%s]" % \
                        task_type_name)
