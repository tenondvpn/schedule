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
import os
import logging
import random
import argparse

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "db_manager.settings")
sys.path.append('../common/db_manager')
import django
django.setup()

sys.path.append('../common')
import task_util
import zk_manager
import zookeeper_path_manager
import handle_ready_tasks
import node_http_server
import common_logger
import check_limit_num

class NodeMain(object):
    """
        主程序入口相关数据初始化
    """
    def __init__(self, config):
        django.setup()
        self.__log = logging

        self.__zk_manager = zk_manager.ZookeeperManager(
                hosts = config.get("zk", "hosts"),
                logger=self.__log)
        # 防止同一个IP启动两个进程，否则会造成任务状态错误，必须避免
        if not self.__can_run():
            raise EnvironmentError("this ip is handling data!")

        # 注意顺序
        self.__check_limit_num = check_limit_num.CheckLimitNum()

        self.__zk_path_mgr = zookeeper_path_manager.ZooKeeperPathManager(
                config,
                self.__zk_manager)
        self.__handle_ready_task = handle_ready_tasks.ReadyTasksCreator(config)
        self.__http_server = node_http_server.NodeHttpServer(
                config,
                self.__handle_ready_task)
        self.__kafka_manager = node_http_server.KafkaRequestManager(config)
        self.__register_signal()

    def start(self):
        self.__log.info("task node started now!")
        self.__zk_path_mgr.start()
        self.__handle_ready_task.start()
        self.__check_limit_num.start()
        self.__log.info("__kafka_manager started now!")
        self.__kafka_manager.start()
        self.__log.info("__http_server started now!")
        self.__http_server.start()

        self.__log.info("task node ended! wait all the thread exit!")

        self.__handle_ready_task.join()
        self.__check_limit_num.join()
        self.__zk_path_mgr.join()
        self.__log.info("task node exit! all threads exited!")
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

    def __can_run(self):
        local_public_ip = config.get("node", "public_ip")
        if local_public_ip is None or local_public_ip.strip() == "":
            local_public_ip = task_util.StaticFunction.get_local_ip()

        ip_lock_path = "%s/%s" % (
                config.get("zk", "ip_lock"),
                local_public_ip)
        print(ip_lock_path)
        if self.__zk_manager.exists(ip_lock_path) is None:
            if self.__zk_manager.create(ip_lock_path) is None:
                self.__log.error("create zk path failed![%s]" % ip_lock_path)
                return False

        node_lock = self.__zk_manager.create_lock(ip_lock_path)
        if node_lock is None:
            self.__log.error("create node lock failed![%s]" % ip_lock_path)
            return False

        return self.__zk_manager.aquire_lock(node_lock, False, None)

def generate_local_network_ip():
    # 选择一个局域网IP段
    segments = [(192, 168), (10, ), (172, 16), (172, 17), (172, 18), (172, 19), (172, 20), (172, 21), (172, 22), (172, 23), (172, 24), (172, 25), (172, 26), (172, 27), (172, 28), (172, 29), (172, 30), (172, 31)]
    first_octet, second_octet = random.choice(segments)
    third_octet = random.randint(0, 255)
    fourth_octet = random.randint(0, 255)
    return f"{first_octet}.{second_octet}.{third_octet}.{fourth_octet}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.description='please enter two parameters a and b ...'
    parser.add_argument("-i", "--tag", help="", dest="tag", type=str, default="")
    args = parser.parse_args()
    if args.tag.strip() == "":
        print("请输入--tag 参数，指定本机的算力标签")
        sys.exit(1)

        
    local_tag = args.tag.strip()
    config = configparser.ConfigParser()
    config.read("./conf/node.conf")

    ip_lock = config.get("zk", "ip_lock").strip()
    pos = ip_lock.rfind('/')
    zk_path = ip_lock[0: pos]
    tag_list = [
            'script',
            'spark',
            'oozie',
            'odps',
            'shell',
            'docker',
            'clickhouse',
            'v100',
            'local_docker',
    ]

    local_public_ip = config.get("node", "public_ip")
    if local_public_ip is None or local_public_ip.strip() == "":
        local_public_ip = task_util.StaticFunction.get_local_ip()

    if task_util.StaticFunction.is_lan(local_public_ip):
        local_public_ip = generate_local_network_ip()
        config.set("node", "public_ip", local_public_ip)

    for tag in tag_list:
        if config.has_option("zk", tag):
             config.set(
                    "zk", 
                    tag, 
                    "%s,%s" % (
                    config.get("zk", tag), 
                    local_tag))
        else:
             config.set(
                    "zk", 
                    tag, 
                    "%s/%s:%s" % (
                    zk_path,
                    tag,
                    local_tag))

    common_logger.init_log(config.get("log", "log_dir"))
    node_main = NodeMain(config)
    if not node_main.start():
        sys.exit(1)
