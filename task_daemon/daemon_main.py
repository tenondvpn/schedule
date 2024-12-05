###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import signal
import sys
import configparser
import logging

import daemon_http_server
import task_manager
sys.path.append('../common')
import task_util
import common_logger

class DaemonMain(object):
    """
        主程序入口相关数据初始化
        此模块是为了避免task_node频繁上线，导致任务失败而存在，
        所以这个模块的初衷就是为了简单，稳定后不需要频繁维护

        部署的时候，同task_node同时部署
    """
    def __init__(self, config):
        self.__log = logging
        self.__task_manager = task_manager.TaskManager(config)
        self.__http_server = daemon_http_server.DaemonHttpServer(
                config, 
                self.__task_manager)
        self.__register_signal()

    def start(self):
        self.__log.info("DaemonMain started now!")
        self.__task_manager.start()
        self.__http_server.start()

        self.__log.info("DaemonMain ended! wait all the thread exit!")
        self.__task_manager.join()
        self.__log.info("DaemonMain exit! all threads exited!")
        return True

    def __sig_handler(self, signum, frame):
        task_util.CONSTANTS.GLOBAL_STOP = True
        self.__http_server.stop()
        self.__log.info(
                "receive a signal %d, will stop process. please wait" % signum)

    def __register_signal(self):
        signal.signal(signal.SIGINT, self.__sig_handler)
        signal.signal(signal.SIGTERM, self.__sig_handler)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("./conf/daemon.conf")
    common_logger.init_log(config.get("log", "log_dir"))

    node_main = DaemonMain(config)
    if not node_main.start():
        sys.exit(1)

