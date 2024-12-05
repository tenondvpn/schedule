###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import signal
import sys
import os
import time
import configparser
import threading
import logging

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "db_manager.settings")
sys.path.append('../common/db_manager')
import django
django.setup()

import task_node_manager
import schedule_creator
import task_dispacher
import check_succeeded_tasks
import check_unexpect_status_tasks
import admin_http_server
import check_uncalled_tasks
import check_limit_num
import check_ordered_tasks

sys.path.append('../common')
import task_util
import zk_manager
import common_logger

class AdminMain(object):
    """
        主程序入口相关数据初始化
    """
    def __init__(self, config):
        django.setup()
        self.__zk_manager = zk_manager.ZookeeperManager(
                hosts=config.get("zk", "hosts"))
        self.__task_node_mgr = task_node_manager.TaskNodeManager(
                config, 
                self.__zk_manager)
        self.__task_dispatcher = task_dispacher.TaskDispatcher(
                self.__task_node_mgr)
        self.__check_succ_task = check_succeeded_tasks.CheckSucceededTasks(
                self.__task_node_mgr)
        self.__check_unexp_task = check_unexpect_status_tasks.\
                CheckUnexpectStatusTasks(self.__task_node_mgr)
        self.__schedule_creator = schedule_creator.ScheduleCreator()
        self.__admin_http_server = admin_http_server.AdminHttpServer(
                config, 
                self.__task_dispatcher)
        self.__register_signal()
        self.__admin_lock = AdminLockChecker(self.__zk_manager, config)
        self.__check_uncalled = check_uncalled_tasks.CheckUncalledTasks(
                self.__task_node_mgr)
        self.__check_limit_num = check_limit_num.CheckLimitNum()
        self.__check_ordered_tasks = check_ordered_tasks.CheckOrderedTasks()
        self.__log = logging

    def start(self):
        self.__schedule_creator.start()
        self.__task_dispatcher.start()
        self.__check_succ_task.start()
        self.__check_unexp_task.start()
        self.__check_uncalled.start()
        self.__check_ordered_tasks.start()
        print("__check_ordered_tasks.start!")
        self.__admin_lock.start()
        print("self.__admin_lock.start!")
        self.__admin_http_server.start()
        self.__log.info("all threads started!")
        print("all threads started!")

        self.__schedule_creator.join()
        self.__log.info("self.__schedule_creator exited!")
        self.__task_dispatcher.join()
        self.__log.info("self.__task_dispatcher exited!")
        self.__check_succ_task.join()
        self.__log.info("self.__check_succ_task exited!")
        self.__check_unexp_task.join()
        self.__log.info("self.__check_unexp_task exited!")
        self.__check_uncalled.join()
        self.__log.info("self.__check_uncalled exited!")
        self.__check_ordered_tasks.join()
        self.__log.info("self.__check_ordered_tasks exited!")
        self.__admin_lock.join()
        self.__log.info("__admin_lock.join exited!")

        self.__log.info("admin exit! all threads exited!")
        return True

    def __sig_handler(self, signum, frame):
        task_util.CONSTANTS.GLOBAL_STOP = True
        self.__admin_http_server.stop()
        self.__log.info("receive a signal %d, "
                      "will stop process. please wait" % signum)

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__sig_handler)
        signal.signal(signal.SIGTERM, self.__sig_handler)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

class AdminLockChecker(threading.Thread):
    """
        检查是否获取锁，并处理数据
    """
    def __init__(self, zk_manager, config, log_conf=''):
        threading.Thread.__init__(self)
        self.__log = logging

        self.__zk_manager = zk_manager
        self.__admin_ip_dir = config.get("zk", "ip_dir")
        if self.__zk_manager.exists(self.__admin_ip_dir) is None:
            raise EnvironmentError("zookeeper has no "
                    "dir:%s!now exit!" % self.__admin_ip_dir)

        self.__admin_http_port = config.get("admin", "admin_http_port")
        local_public_ip = config.get("admin", "public_ip")
        if local_public_ip is None or local_public_ip.strip() == "":
            local_public_ip = task_util.StaticFunction.get_local_ip()
        self.__ip_dir = "%s/%s:%s:" % (
                self.__admin_ip_dir.strip(), 
                local_public_ip,
                self.__admin_http_port.strip())
        if self.__zk_manager.create(
                path=self.__ip_dir,
                ephemeral=True,
                sequence=True) is None:
            raise EnvironmentError(
                    "create zk path failed![%s]" % self.__ip_dir)

    def run(self):
        while not task_util.CONSTANTS.GLOBAL_STOP:
            if not self.__check_getted_lock():
                task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK = True
                self.__log.info("waiting to get lock!")
            else:
                task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK = False
                self.__log.info("this admin has getted lock!")
            time.sleep(10)

    def __check_getted_lock(self):
        children = self.__zk_manager.get_children(self.__admin_ip_dir)
        if children is None:
            return False
        min_index = None
        self_index = None
        local_public_ip = config.get("admin", "public_ip")
        if local_public_ip is None or local_public_ip.strip() == "":
            local_public_ip = task_util.StaticFunction.get_local_ip()
        for child in children:
            field_list = child.split(":")
            if len(field_list) != 3:
                self.__log.error("wrong child lock[%s]" % child)
                continue            
            
            if child.find(local_public_ip) != -1:
                self_index = field_list[2].strip()

            if min_index is None:
                min_index = field_list[2].strip() 
                continue

            if field_list[2].strip() < min_index:
                min_index = field_list[2].strip()

        if self_index is None:
            if self.__zk_manager.create(
                    path=self.__ip_dir,
                    ephemeral=True,
                    sequence=True) is None:
                self.__log.error(
                        "create zk path failed![%s]" % self.__ip_dir)
            return False

        if min_index == self_index:
            return True
        return False

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("./conf/admin.conf")
    common_logger.init_log(config.get("log", "log_dir"))

    admin_main = AdminMain(config)
    if not admin_main.start():
        sys.exit(1)

