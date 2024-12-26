###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import traceback
import threading

import kazoo.client
import kazoo.recipe.watchers
import kazoo.recipe.lock
import kazoo.exceptions
import kazoo.protocol.states

from horae import tools_util

class ZookeeperManager(tools_util.Singleton):
    """
        zookeeper的管理类
        1.连接zookeeper，同时检查其连接状态，如果连接断开则重连
        2.管理相关路径信息
        3.锁机制的管理
        4.监控子节点信息变化
        5.捕获所有异常，并且error
    """
    def __init__(
            self, 
            hosts,
            timeout=10.0, 
            client_id=None, 
            handler=None,
            default_acl=None, 
            auth_data=None, 
            read_only=None,
            randomize_hosts=True, 
            connection_retry=None,
            command_retry=None, 
            logger=None, 
            **kwargs):
        if not hasattr(self, "a"):
            self.__log = logger
            self.__condition = threading.Condition
            if hosts.strip() == '':
                self.__log.error("zookeeper hosts is empty!")
                raise kazoo.exceptions.ZookeeperError("zookeeper hosts is empty!")
            self.__hosts = hosts
            self.__timeout = timeout
            self.__client_id=client_id
            self.__handler=handler
            self.__default_acl=default_acl
            self.__auth_data=auth_data
            self.__read_only=read_only
            self.__randomize_hosts=randomize_hosts
            self.__connection_retry=connection_retry
            self.__command_retry=command_retry

            self.__connect_to_zookeeper()

            self.a = "a"  # 必须在后面

    def __connect_to_zookeeper(self):
        self.__zk = kazoo.client.KazooClient(
                hosts=self.__hosts, 
                timeout=self.__timeout, 
                client_id=self.__client_id, 
                handler=self.__handler, 
                default_acl=self.__default_acl, 
                auth_data=self.__auth_data, 
                read_only=self.__read_only, 
                randomize_hosts=self.__randomize_hosts, 
                connection_retry=self.__connection_retry, 
                command_retry=self.__command_retry, 
                logger=self.__log)
        self.__zk.add_listener(self.__connect_state_listener)
        self.__zk.start()     

    def __connect_state_listener(self, state):
        if state in (
                kazoo.protocol.states.KazooState.LOST, 
                kazoo.protocol.states.KazooState.SUSPENDED):
            self.__log.error("connecting to zookeeper failed! will restart")
            # self.__zk.restart()
            # 经测试，发现重连有问题，所以此处error后直接返回
            # retry_count = 0
            # while not tools_util.CONSTANTS.GLOBAL_STOP:
            #     try:
            #         self.__connect_to_zookeeper()
            #         break
            #     except Exception as ex:
            #         self.__log.error("zk restart failed[%s], "
            #                 "will retried:%d[trace:%s]" % (
            #                 str(ex), retry_count, traceback.format_exc()))
            #         retry_count = retry_count + 1
            #     time.sleep(1)

    def exists(self, path):
        try:
            return self.__zk.exists(path)
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def create(
            self, 
            path, 
            value=b"", 
            acl=None, 
            ephemeral=False,
            sequence=False, 
            makepath=False):
        try:
            return self.__zk.create(
                    path, 
                    value, 
                    acl, 
                    ephemeral, 
                    sequence, 
                    makepath)
        except kazoo.exceptions.NodeExistsError as ex:
            self.__log.error("exists has NodeExistsError:" + str(ex))
            return None
        except kazoo.exceptions.NoNodeError as ex:
            self.__log.error("exists has NoNodeError:" + str(ex))
            return None
        except kazoo.exceptions.NoChildrenForEphemeralsError as ex:
            self.__log.error("exists has NoChildrenForEphemeralsErr:" + str(ex))
            return None
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def delete(self, path, version=-1, recursive=False):
        try:
            return self.__zk.delete(path, version, recursive)
        except kazoo.exceptions.BadVersionError as ex:
            self.__log.error("exists has BadVersionError:" + str(ex))
            return None
        except kazoo.exceptions.NoNodeError as ex:
            self.__log.error("exists has NoNodeError:" + str(ex))
            return None
        except kazoo.exceptions.NotEmptyError as ex:
            self.__log.error("exists has NotEmptyError:" + str(ex))
            return None
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def set(self, path, value, version=-1):
        try:
            return self.__zk.set(path, value, version)
        except kazoo.exceptions.BadVersionError as ex:
            self.__log.error("exists has BadVersionError:" + str(ex))
            return None
        except kazoo.exceptions.NoNodeError as ex:
            self.__log.error("exists has NoNodeError:" + str(ex))
            return None
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def get(self, path):
        try:
            return self.__zk.get(path)
        except kazoo.exceptions.BadVersionError as ex:
            self.__log.error("exists has BadVersionError:" + str(ex))
            return None
        except kazoo.exceptions.NoNodeError as ex:
            self.__log.error("exists has NoNodeError:" + str(ex))
            return None
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def get_children(self, path, include_data=False):
        try:
            return self.__zk.get_children(path, None, include_data)
        except kazoo.exceptions.NoNodeError as ex:
            self.__log.error("exists has NoNodeError:" + str(ex))
            return None
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def watch_children(self, path, watch_func):
        try:
            return kazoo.recipe.watchers.ChildrenWatch(
                    self.__zk, 
                    path, 
                    watch_func)
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def watch_node(self, path, watch_func):
        try:
            return kazoo.recipe.watchers.DataWatch(self.__zk, path, watch_func)
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def ensure_path(self, path, acl=None):
        try:
            return self.__zk.ensure_path(path, acl)
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    def create_lock(self, lock_path):
        try:
            return kazoo.recipe.lock.Semaphore(self.__zk, lock_path)
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return None
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return None

    # 如果连接断开，获取锁返回False，当重新连上后，会成功返回锁状态
    def aquire_lock(self, lock_handler, blocking=True, timeout=None):
        if lock_handler is None:
            self.__log.error("this mananger has not create lock handler!")
            return False
        try:
            return lock_handler.acquire(blocking, timeout)
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return False
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return False

    # 如果连接断开，获取锁返回False，当重新连上后，会成功返回锁状态
    def release_lock(self, lock_handler):
        if lock_handler is None:
            self.__log.error("this mananger has not create lock handler!")
            return False
        try:
            return lock_handler.release()
        except kazoo.exceptions.ZookeeperError as ex:
            self.__log.error("exists has ZookeeperError:" + str(ex))
            return False
        except Exception as ex:
            self.__log.error("exists has Exception[%s]:[trace:%s]" % (
                    str(ex), traceback.format_exc()))
            return False

